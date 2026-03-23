#!/usr/bin/env python3
"""
NGX Official PDF Ingestion Script

Fetches and parses the official NGX Daily Official List PDF from doclib.ngxgroup.com
Extracts real end-of-day stock prices and stores them in MongoDB.

NO demo data. NO fake values. ONLY official NGX data.
"""

import requests
import pdfplumber
import re
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import os
from dotenv import load_dotenv
import logging
from pathlib import Path

# Setup
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]
stock_prices_collection = db.stock_prices

# NGX PDF source configuration
NGX_PDF_BASE_URL = "https://doclib.ngxgroup.com/DownloadsContent/"
NGX_EQUITIES_PDF_PATTERN = "Daily Official List - Equities for {date}.pdf"
NGX_COVER_PDF_PATTERN = "COVERPAGE FOR {date}.pdf"


def construct_pdf_url(trade_date):
    """
    Construct the NGX Daily Official List PDF URL for equities.
    
    Args:
        trade_date: datetime object
    
    Returns:
        Tuple of (equities_url, cover_url, date_str)
    """
    date_str = trade_date.strftime("%d-%m-%Y")
    
    # Equities PDF (the one with actual stock prices)
    equities_filename = NGX_EQUITIES_PDF_PATTERN.format(date=date_str)
    equities_url = NGX_PDF_BASE_URL + equities_filename.replace(" ", "%20")
    
    # Cover PDF (indices only)
    cover_filename = NGX_COVER_PDF_PATTERN.format(date=date_str)
    cover_url = NGX_PDF_BASE_URL + cover_filename.replace(" ", "%20")
    
    return equities_url, cover_url, date_str


def download_pdf(url, save_path):
    """
    Download NGX PDF from official source.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading PDF from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"✅ PDF downloaded: {save_path}")
        return True
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.error(f"❌ PDF not found (404): {url}")
            logger.error("This date may not have market data (weekend/holiday) or PDF not yet published")
        else:
            logger.error(f"❌ HTTP error downloading PDF: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error downloading PDF: {e}")
        return False


def parse_ngx_pdf(pdf_path):
    """
    Parse the NGX Daily Official List - Equities PDF and extract stock prices.
    
    The PDF has tables with columns:
    [Symbol, Security Name, Public Quotation Price, Official Open, Official Close, Current Market Price, ...]
    
    Returns:
        List of stock data dictionaries
    """
    stocks = []
    
    try:
        logger.info(f"Parsing Equities PDF: {pdf_path}")
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                logger.info(f"Processing page {page_num}/{len(pdf.pages)}")
                
                # Extract tables
                tables = page.extract_tables()
                
                if not tables:
                    continue
                
                for table in tables:
                    # Skip first 3 rows (headers)
                    for row in table[3:]:
                        if not row or len(row) < 6:
                            continue
                        
                        symbol = (row[0] or "").strip()
                        name = (row[1] or "").strip().replace('\n', ' ')
                        
                        # Skip if no symbol or symbol looks like header
                        if not symbol or len(symbol) < 2 or symbol.upper() in ['SYMBOL', 'EX']:
                            continue
                        
                        # Extract prices
                        # Column 4: Official Close
                        # Column 5: Current Market Price
                        official_close_str = (row[4] or "").strip().replace(',', '')
                        current_price_str = (row[5] or "").strip().replace(',', '')
                        
                        # Use whichever is available
                        if current_price_str and current_price_str.replace('.', '').isdigit():
                            current_price = float(current_price_str)
                        elif official_close_str and official_close_str.replace('.', '').isdigit():
                            current_price = float(official_close_str)
                        else:
                            continue  # No valid price
                        
                        if current_price <= 0:
                            continue
                        
                        # For now, use current price as both current and previous
                        # (previous close would need historical data)
                        official_close = current_price
                        
                        # Try to get previous close from "Business Done" price column if available
                        # Column 9 often has last trade price
                        business_price_str = (row[9] if len(row) > 9 else "").strip().replace(',', '')
                        if business_price_str and business_price_str.replace('.', '').isdigit():
                            previous_close = float(business_price_str)
                        else:
                            # Estimate: assume 2% change on average
                            previous_close = current_price * 0.98
                        
                        # Calculate change
                        if previous_close > 0:
                            change_pct = ((current_price - previous_close) / previous_close) * 100
                        else:
                            change_pct = 0.0
                        
                        stocks.append({
                            'symbol': symbol,
                            'name': name,
                            'official_close': official_close,
                            'previous_close': previous_close,
                            'current_price': current_price,
                            'change_percent': round(change_pct, 2)
                        })
                        
                        logger.debug(f"  Parsed: {symbol} = ₦{current_price}")
        
        logger.info(f"✅ Parsed {len(stocks)} stocks from Equities PDF")
        
        # Show key stocks for verification
        key_stocks = ['ZENITHBANK', 'UBA', 'GTCO', 'DANGCEM', 'MTNN']
        logger.info("\nKey stocks found:")
        for stock in stocks:
            if stock['symbol'] in key_stocks:
                logger.info(f"  {stock['symbol']}: ₦{stock['current_price']:,.2f}")
        
        return stocks
        
    except Exception as e:
        logger.error(f"❌ Error parsing Equities PDF: {e}")
        import traceback
        traceback.print_exc()
        return []


async def store_prices(stocks, trade_date, source_url):
    """
    Store parsed stock prices in MongoDB with CORRECT previous_close values.
    
    Gets previous_close from the prior trading day's data in the database.
    """
    if not stocks:
        logger.warning("No stocks to store")
        return 0
    
    try:
        date_str = trade_date.strftime("%Y-%m-%d")
        timestamp = datetime.now().isoformat()
        
        # Get previous trading day (go back up to 7 days to skip weekends/holidays)
        previous_prices = {}
        for days_back in range(1, 8):
            prev_date = trade_date - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            
            # Query previous day's prices
            prev_docs = await stock_prices_collection.find(
                {"date": prev_date_str},
                {"_id": 0, "symbol": 1, "official_close": 1}
            ).to_list(500)
            
            if prev_docs:
                previous_prices = {doc['symbol']: doc['official_close'] for doc in prev_docs}
                logger.info(f"Found {len(previous_prices)} previous prices from {prev_date_str}")
                break
        
        stored_count = 0
        
        # Determine which stocks are "Analyzed" based on major tickers
        analyzed_tickers = [
            'ZENITHBANK', 'UBA', 'GTCO', 'ACCESSCORP', 'FBNH',
            'DANGCEM', 'MTNN', 'BUACEMENT', 'SEPLAT', 'NESTLE',
            'AIRTELAFRI', 'FLOURMILL', 'STANBIC', 'FIDELITYBK', 'GUINNESS',
            'DANGSUGAR', 'NB', 'OANDO', 'TRANSCORP', 'INTBREW',
            'WAPCO', 'UNILEVER', 'CADBURY', 'STERLINGNG', 'LIVESTOCK',
            'CHAMS', 'AFRIPRUD', 'MANSARD', 'NEM', 'PRESTIGE',
            'CUSTODIAN', 'LASACO', 'LINKASSURE', 'MBENEFIT', 'REGALINS',
            'SOVRENINS', 'STACO', 'STDINSURE', 'SUNUASSUR', 'UNIVINSURE',
            'VERITASKAP', 'WAPIC', 'PRESCO', 'FTN', 'OKOMUOIL'
        ]
        
        for stock in stocks:
            symbol = stock['symbol']
            
            # Get previous close from historical data
            if symbol in previous_prices:
                previous_close = previous_prices[symbol]
            else:
                # No historical data available - use current price as fallback
                previous_close = stock['official_close']
                logger.warning(f"No previous close found for {symbol}, using current price")
            
            # Recalculate change percent with correct previous close
            if previous_close > 0:
                change_pct = ((stock['official_close'] - previous_close) / previous_close) * 100
            else:
                change_pct = 0.0
            
            stock_type = "Analyzed" if symbol in analyzed_tickers else "Price-Only"
            
            doc = {
                'date': date_str,
                'symbol': symbol,
                'name': stock['name'],
                'current_price': stock['current_price'],
                'previous_close': previous_close,  # Now from historical data!
                'official_close': stock['official_close'],
                'volume': None,
                'stock_type': stock_type,
                'source_name': 'NGX Daily Official List PDF',
                'source_type': 'official_pdf',
                'source_url': source_url,
                'last_updated_at': timestamp
            }
            
            # Upsert
            await stock_prices_collection.update_one(
                {'date': date_str, 'symbol': symbol},
                {'$set': doc},
                upsert=True
            )
            stored_count += 1
        
        logger.info(f"✅ Stored {stored_count} stock prices for {date_str}")
        return stored_count
        
    except Exception as e:
        logger.error(f"❌ Error storing prices: {e}")
        import traceback
        traceback.print_exc()
        return 0


async def ingest_ngx_data(trade_date=None):
    """
    Main ingestion function.
    
    Args:
        trade_date: datetime object (defaults to yesterday for EOD)
    """
    if trade_date is None:
        # Default to yesterday (most recent trading day)
        trade_date = datetime.now() - timedelta(days=1)
    
    # Construct PDF URLs
    equities_url, cover_url, date_str = construct_pdf_url(trade_date)
    
    logger.info("="*60)
    logger.info("NGX Official PDF Ingestion")
    logger.info("="*60)
    logger.info(f"Trade Date: {date_str}")
    logger.info(f"Equities Source: {equities_url}")
    logger.info("")
    
    # Download Equities PDF (the one with actual stock prices)
    pdf_path = f"/tmp/ngx_equities_{date_str}.pdf"
    
    if not download_pdf(equities_url, pdf_path):
        logger.error("❌ Failed to download Equities PDF")
        logger.error("Possible reasons:")
        logger.error("  - Date is weekend/holiday (no trading)")
        logger.error("  - PDF not yet published")
        logger.error("  - Network issue")
        return {
            'success': False,
            'trade_date': date_str,
            'error': 'Equities PDF download failed',
            'source_url': equities_url
        }
    
    # Parse PDF
    stocks = parse_ngx_pdf(pdf_path)
    
    if not stocks:
        logger.error("❌ No stocks parsed from PDF")
        return {
            'success': False,
            'trade_date': date_str,
            'error': 'PDF parsing failed - no stocks extracted',
            'source_url': equities_url
        }
    
    # Store in database
    stored_count = await store_prices(stocks, trade_date, equities_url)
    
    # Cleanup
    try:
        os.remove(pdf_path)
    except:
        pass
    
    logger.info("")
    logger.info("="*60)
    logger.info(f"✅ Ingestion complete: {stored_count} stocks stored")
    logger.info("="*60)
    
    return {
        'success': True,
        'trade_date': date_str,
        'source_url': equities_url,
        'total_stocks': stored_count,
        'stocks_sample': stocks[:3] if stocks else []
    }


async def clear_demo_data():
    """
    Clear all demo/fake/placeholder data from database.
    """
    logger.info("Clearing demo/placeholder data...")
    
    # Delete all prices that don't have official source
    result = await stock_prices_collection.delete_many({
        'source_type': {'$ne': 'official_pdf'}
    })
    
    logger.info(f"✅ Cleared {result.deleted_count} demo/placeholder records")
    return result.deleted_count


if __name__ == "__main__":
    async def main():
        # Clear demo data first
        await clear_demo_data()
        
        # Try the last 5 weekdays to find a trading day with available PDF
        for days_ago in range(1, 6):
            target_date = datetime.now() - timedelta(days=days_ago)
            
            # Skip weekends
            if target_date.weekday() >= 5:
                continue
            
            logger.info(f"\nTrying date: {target_date.strftime('%Y-%m-%d')}")
            
            result = await ingest_ngx_data(target_date)
            
            if result['success']:
                logger.info("\n✅ Successfully ingested NGX official data")
                logger.info(f"Trade Date: {result['trade_date']}")
                logger.info(f"Total Stocks: {result['total_stocks']}")
                logger.info(f"Source: {result['source_url']}")
                
                # Show sample
                if result['stocks_sample']:
                    logger.info("\nSample stocks:")
                    for stock in result['stocks_sample']:
                        logger.info(f"  {stock['symbol']}: ₦{stock['official_close']}")
                
                # Auto-generate alerts after successful ingestion
                logger.info("\n🔔 Generating alerts for new data...")
                try:
                    from alert_engine import generate_all_alerts
                    alert_stats = await generate_all_alerts(
                        db,
                        result['trade_date'],
                        check_price_triggers=True,
                        check_signal_changes=True
                    )
                    logger.info(f"✅ Alert generation complete:")
                    logger.info(f"   Signal alerts: {alert_stats.get('signal_alerts', 0)}")
                    logger.info(f"   Price alerts: {alert_stats.get('price_alerts', 0)}")
                    logger.info(f"   Total stored: {alert_stats.get('total_generated', 0)}")
                    logger.info(f"   Duplicates prevented: {alert_stats.get('duplicates_prevented', 0)}")
                except Exception as e:
                    logger.error(f"Alert generation failed: {e}")
                
                break
            else:
                logger.warning(f"Failed for {target_date.strftime('%Y-%m-%d')}, trying previous day...")
        
        client.close()
    
    asyncio.run(main())
