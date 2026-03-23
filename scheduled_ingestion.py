"""
NGX Scheduled Ingestion - APScheduler Integration
=================================================
Runs automatic data ingestion at 5:00 PM WAT (4:00 PM UTC) daily.

This scheduler runs within the FastAPI process, so it:
- Starts when the server starts
- Stops when the server stops
- Persists across hot reloads (with proper shutdown handling)

WAT (West Africa Time) = UTC + 1 hour
5:00 PM WAT = 4:00 PM UTC = 16:00 UTC
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from motor.motor_asyncio import AsyncIOMotorClient
import os

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler = None
_is_initialized = False


async def run_scheduled_ingestion():
    """
    Main scheduled ingestion task.
    Runs at 5:00 PM WAT (4:00 PM UTC) daily.
    """
    from dotenv import load_dotenv
    from pathlib import Path
    
    ROOT_DIR = Path(__file__).parent
    load_dotenv(ROOT_DIR / '.env')
    
    logger.info("="*60)
    logger.info("SCHEDULED INGESTION - STARTING")
    logger.info(f"Trigger time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("="*60)
    
    client = None
    try:
        # Connect to MongoDB
        mongo_url = os.environ['MONGO_URL']
        db_name = os.environ['DB_NAME']
        client = AsyncIOMotorClient(mongo_url)
        db = client[db_name]
        
        # Import ingestion modules
        from ngx_pdf_ingestion import ingest_ngx_data, construct_pdf_url
        from ngx_signal_generator import main as generate_signals
        from alert_engine import generate_all_alerts
        
        # Determine target date (today if after market close, else the last trading day)
        now_utc = datetime.now(timezone.utc)
        now_wat = now_utc + timedelta(hours=1)  # WAT = UTC+1
        
        # After 5 PM WAT, try today's data; before that, find the last trading day
        if now_wat.hour >= 17:
            target_date = now_wat.date()
        else:
            target_date = (now_wat - timedelta(days=1)).date()
        
        # Find the most recent trading day (skip weekends)
        target_datetime = datetime.combine(target_date, datetime.min.time())
        while target_datetime.weekday() >= 5:  # Saturday (5) or Sunday (6)
            target_datetime = target_datetime - timedelta(days=1)
        target_date = target_datetime.date()
        
        logger.info(f"Target trading date: {target_date} (today WAT: {now_wat.date()})")
        
        # Check if we already have this date's data
        existing = await db.stock_prices.find_one(
            {"date": str(target_date)},
            {"_id": 0, "date": 1}
        )
        
        if existing:
            logger.info(f"Data for {target_date} already exists. Checking for updates...")
        
        # Run ingestion
        logger.info(f"Attempting ingestion for: {target_date}")
        result = await ingest_ngx_data(target_datetime)
        
        if result['success']:
            logger.info(f"SUCCESS: Ingested {result['total_stocks']} stocks for {result['trade_date']}")
            
            # Generate signals
            logger.info("Generating trading signals...")
            try:
                # Run signal generator
                await generate_signals()
                logger.info("Signals generated successfully")
            except Exception as e:
                logger.error(f"Signal generation failed: {e}")
            
            # Generate alerts
            logger.info("Generating alerts...")
            try:
                alert_stats = await generate_all_alerts(
                    db,
                    result['trade_date'],
                    check_price_triggers=True,
                    check_signal_changes=True
                )
                logger.info(f"Generated {alert_stats.get('total_generated', 0)} alerts")
            except Exception as e:
                logger.error(f"Alert generation failed: {e}")
            
            # Update status
            await update_ingestion_status(db, {
                "status": "updated",
                "trade_date": result['trade_date'],
                "last_successful_update": now_utc.isoformat(),
                "last_attempted_run": now_utc.isoformat(),
                "last_attempted_trade_date": str(target_date),
                "stocks_ingested": result['total_stocks'],
                "source_url": result['source_url'],
                "is_stale": False,
                "message": f"Successfully ingested {result['total_stocks']} stocks"
            })
            
            logger.info("SCHEDULED INGESTION - COMPLETED SUCCESSFULLY")
            return {"success": True, **result}
        
        else:
            logger.warning(f"INGESTION FAILED: {result.get('error', 'Unknown error')}")
            
            # Update status but preserve existing data
            await update_ingestion_status(db, {
                "status": "no_new_data",
                "last_attempted_run": now_utc.isoformat(),
                "last_attempted_trade_date": str(target_date),
                "error": result.get('error'),
                "message": f"No new data available for {target_date}"
            })
            
            logger.info("SCHEDULED INGESTION - NO NEW DATA")
            return {"success": False, **result}
            
    except Exception as e:
        logger.error(f"SCHEDULED INGESTION ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        if client:
            try:
                db = client[os.environ['DB_NAME']]
                await update_ingestion_status(db, {
                    "status": "failed",
                    "last_attempted_run": datetime.now(timezone.utc).isoformat(),
                    "error": str(e),
                    "message": f"Ingestion failed: {str(e)}"
                })
            except:
                pass
        
        return {"success": False, "error": str(e)}
    
    finally:
        if client:
            client.close()
        logger.info("="*60)


async def update_ingestion_status(db, updates: dict):
    """Update ingestion status document."""
    await db.ingestion_status.update_one(
        {"_id": "ingestion_status"},
        {"$set": updates},
        upsert=True
    )


def start_scheduler():
    """
    Start the APScheduler with WAT timezone scheduling.
    Called on FastAPI startup.
    """
    global scheduler, _is_initialized
    
    if _is_initialized and scheduler and scheduler.running:
        logger.info("Scheduler already running")
        return scheduler
    
    try:
        scheduler = AsyncIOScheduler()
        
        # Schedule job for 5:00 PM WAT = 4:00 PM UTC (16:00)
        # Also add a backup run at 6:00 PM WAT in case of delays
        scheduler.add_job(
            run_scheduled_ingestion,
            CronTrigger(hour=16, minute=0, timezone='UTC'),  # 5:00 PM WAT
            id='ngx_daily_ingestion_main',
            name='NGX Daily Ingestion (5:00 PM WAT)',
            replace_existing=True,
            misfire_grace_time=3600  # Allow 1 hour grace period
        )
        
        # Backup run at 6:00 PM WAT
        scheduler.add_job(
            run_scheduled_ingestion,
            CronTrigger(hour=17, minute=0, timezone='UTC'),  # 6:00 PM WAT
            id='ngx_daily_ingestion_backup',
            name='NGX Daily Ingestion Backup (6:00 PM WAT)',
            replace_existing=True,
            misfire_grace_time=3600
        )
        
        scheduler.start()
        _is_initialized = True
        
        logger.info("="*60)
        logger.info("NGX SCHEDULER STARTED")
        logger.info("Scheduled jobs:")
        for job in scheduler.get_jobs():
            logger.info(f"  - {job.name}: {job.trigger}")
        logger.info("="*60)
        
        return scheduler
        
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        import traceback
        traceback.print_exc()
        return None


def stop_scheduler():
    """
    Stop the scheduler gracefully.
    Called on FastAPI shutdown.
    """
    global scheduler, _is_initialized
    
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("NGX Scheduler stopped")
    
    _is_initialized = False


def get_scheduler_status():
    """Get current scheduler status."""
    global scheduler
    
    if not scheduler:
        return {
            "running": False,
            "jobs": [],
            "message": "Scheduler not initialized"
        }
    
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "trigger": str(job.trigger),
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None
        })
    
    return {
        "running": scheduler.running,
        "jobs": jobs,
        "message": "Scheduler active" if scheduler.running else "Scheduler stopped"
    }


async def run_ingestion_now():
    """
    Run ingestion immediately (for manual trigger).
    Returns the result of the ingestion.
    """
    logger.info("Manual ingestion triggered")
    return await run_scheduled_ingestion()
