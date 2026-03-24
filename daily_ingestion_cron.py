#!/usr/bin/env python3
"""
NGX Daily Ingestion Script - Standalone for Cron
================================================
Run this script daily at 5:00 PM WAT via system cron.

Cron entry (add via `crontab -e`):
0 17 * * * cd /app/backend && /usr/bin/python3 daily_ingestion_cron.py >> /var/log/ngx_ingestion.log 2>&1

Features:
- Fail-safe: Never overwrites existing data if new data not found
- Logs all operations for audit trail
- Updates ingestion status in database
- Generates signals and alerts after successful ingestion
"""

import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / '.env')

from motor.motor_asyncio import AsyncIOMotorClient

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
db_name = os.environ['DB_NAME']


# Nigerian public holidays 2024-2025 (NGX closed)
NIGERIAN_HOLIDAYS = [
    "2024-01-01",  # New Year
    "2024-03-29",  # Good Friday
    "2024-04-01",  # Easter Monday
    "2024-05-01",  # Workers Day
    "2024-05-27",  # Children's Day
    "2024-06-12",  # Democracy Day
    "2024-10-01",  # Independence Day
    "2024-12-25",  # Christmas
    "2024-12-26",  # Boxing Day
    # 2025 holidays
    "2025-01-01",  # New Year
    "2025-04-18",  # Good Friday
    "2025-04-21",  # Easter Monday
    "2025-05-01",  # Workers Day
    "2025-05-27",  # Children's Day
    "2025-06-12",  # Democracy Day
    "2025-10-01",  # Independence Day
    "2025-12-25",  # Christmas
    "2025-12-26",  # Boxing Day
]


def is_trading_day(date: datetime) -> bool:
    """Check if a given date is a trading day (weekday and not a holiday)."""
    # Check if weekend
    if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Check if Nigerian holiday
    date_str = date.strftime("%Y-%m-%d")
    if date_str in NIGERIAN_HOLIDAYS:
        return False
    
    return True


def get_expected_trade_date() -> datetime:
    """
    Get the expected trade date for data.
    If current time is after 5 PM WAT, expect today's data.
    Otherwise, expect yesterday's data.
    """
    # WAT is UTC+1
    now_utc = datetime.now(timezone.utc)
    now_wat = now_utc + timedelta(hours=1)
    
    # If it's after 5 PM WAT, we should have today's data
    if now_wat.hour > 18 or (now_wat.hour == 18 and now_wat.minute >= 30):
        target_date = now_wat.date()
    else:
        # Before 5 PM, expect yesterday's data
        target_date = (now_wat - timedelta(days=1)).date()
    
    # Find the most recent trading day
    target = datetime.combine(target_date, datetime.min.time())
    while not is_trading_day(target):
        target = target - timedelta(days=1)
    
    return target


async def update_ingestion_status(db, status: dict):
    """Update the ingestion status document in MongoDB."""
    status_doc = {
        "_id": "ingestion_status",
        "last_check_time": datetime.now(timezone.utc).isoformat(),
        **status
    }
    
    await db.ingestion_status.update_one(
        {"_id": "ingestion_status"},
        {"$set": status_doc},
        upsert=True
    )
    logger.info(f"Updated ingestion status: {status.get('status')}")


async def get_existing_data_date(db) -> str:
    """Get the most recent trade date with data in the database."""
    latest = await db.stock_prices.find_one(
        {},
        {"_id": 0, "date": 1},
        sort=[("date", -1)]
    )
    return latest["date"] if latest else None


async def run_ingestion():
    """
    Main ingestion function with fail-safe logic.
    
    FAIL-SAFE RULES:
    1. If PDF not found -> DO NOT overwrite existing data
    2. If parsing fails -> DO NOT overwrite existing data
    3. If no stocks extracted -> DO NOT overwrite existing data
    4. Only store new data if successfully parsed
    """
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    logger.info("="*60)
    logger.info("NGX DAILY INGESTION - STARTED")
    logger.info("="*60)
    logger.info(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    
    try:
        # Get existing data date
        existing_date = await get_existing_data_date(db)
        logger.info(f"Existing data date: {existing_date}")
        
        # Determine expected trade date
        expected_date = get_expected_trade_date()
        expected_date_str = expected_date.strftime("%Y-%m-%d")
        logger.info(f"Expected trade date: {expected_date_str}")
        
        # Check if we already have today's data
        if existing_date == expected_date_str:
            logger.info(f"Data for {expected_date_str} already exists. Skipping ingestion.")
            await update_ingestion_status(db, {
                "status": "already_updated",
                "trade_date": existing_date,
                "last_successful_update": datetime.now(timezone.utc).isoformat(),
                "is_stale": False,
                "message": f"Data for {expected_date_str} already ingested"
            })
            client.close()
            return {"success": True, "status": "already_updated"}
        
        # Import and run the NGX PDF ingestion
        from ngx_pdf_ingestion import ingest_ngx_data
        
        # Try to ingest data for the expected date
        result = await ingest_ngx_data(expected_date)
        
        if result['success']:
            logger.info(f"SUCCESS: Ingested {result['total_stocks']} stocks for {result['trade_date']}")
            
            # Generate signals after successful ingestion
            logger.info("Generating trading signals...")
            try:
                from ngx_signal_generator import run_signal_generation
                signal_result = await run_signal_generation(db, result['trade_date'])
                logger.info(f"Generated {signal_result.get('total_signals', 0)} signals")
            except Exception as e:
                logger.error(f"Signal generation failed: {e}")
            
            # Generate alerts
            logger.info("Generating alerts...")
            try:
                from alert_engine import generate_all_alerts
                alert_stats = await generate_all_alerts(
                    db,
                    result['trade_date'],
                    check_price_triggers=True,
                    check_signal_changes=True
                )
                logger.info(f"Generated {alert_stats.get('total_generated', 0)} alerts")
            except Exception as e:
                logger.error(f"Alert generation failed: {e}")
            
            # Update status to success
            await update_ingestion_status(db, {
                "status": "updated",
                "trade_date": result['trade_date'],
                "last_successful_update": datetime.now(timezone.utc).isoformat(),
                "stocks_ingested": result['total_stocks'],
                "source_url": result['source_url'],
                "is_stale": False,
                "message": f"Successfully ingested {result['total_stocks']} stocks"
            })
            
            client.close()
            return {"success": True, "status": "updated", **result}
        
        else:
            # FAIL-SAFE: Do NOT overwrite existing data
            logger.warning(f"FAIL-SAFE: Ingestion failed for {expected_date_str}")
            logger.warning(f"Error: {result.get('error', 'Unknown')}")
            logger.warning(f"Preserving existing data from: {existing_date}")
            
            # Calculate if data is stale (more than 2-3 trading days old)
            is_stale = False
            if existing_date:
                existing_dt = datetime.strptime(existing_date, "%Y-%m-%d")
                trading_days_diff = 0
                check_date = expected_date
                while check_date > existing_dt and trading_days_diff < 5:
                    check_date = check_date - timedelta(days=1)
                    if is_trading_day(check_date):
                        trading_days_diff += 1
                
                # Stale if more than 2 trading days old
                is_stale = trading_days_diff > 2
                logger.info(f"Trading days since last update: {trading_days_diff}, Is stale: {is_stale}")
            
            await update_ingestion_status(db, {
                "status": "no_new_data",
                "trade_date": existing_date,
                "expected_date": expected_date_str,
                "last_check_time": datetime.now(timezone.utc).isoformat(),
                "is_stale": is_stale,
                "error": result.get('error'),
                "message": f"No new data available. Using last data from {existing_date}"
            })
            
            client.close()
            return {"success": False, "status": "no_new_data", "existing_date": existing_date}
    
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        # Update status to failed
        try:
            await update_ingestion_status(db, {
                "status": "failed",
                "trade_date": existing_date if 'existing_date' in locals() else None,
                "last_check_time": datetime.now(timezone.utc).isoformat(),
                "is_stale": True,
                "error": str(e),
                "message": f"Ingestion failed: {str(e)}"
            })
        except Exception:
            pass
        
        client.close()
        return {"success": False, "status": "failed", "error": str(e)}
    
    finally:
        logger.info("="*60)
        logger.info("NGX DAILY INGESTION - COMPLETED")
        logger.info("="*60)


if __name__ == "__main__":
    result = asyncio.run(run_ingestion())
    print(json.dumps(result, indent=2, default=str))
    
    # Exit with appropriate code for cron
    sys.exit(0 if result.get('success') or result.get('status') == 'no_new_data' else 1)
