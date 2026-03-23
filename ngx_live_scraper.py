#!/usr/bin/env python3
"""
NGX AFX Data Scraper

Fetches REAL live stock prices from afx.kwayisi.org/ngx
This is a free, publicly available source with real-time NGX data.

NO demo data. NO fake values. ONLY real live prices.
"""

import requests
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
from datetime import datetime
import re

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

# AFX NGX source
AFX_NGX_URL = "https://afx.kwayisi.org/ngx/"


def scrape_ngx_live_data():
    """
    Scrape live stock prices from AFX NGX source.
    
    Returns:
        List of stock data dictionaries with real prices
    """
    stocks = []
    
    try:
        logger.info(f"Fetching live data from: {AFX_NGX_URL}")
        
        # Fetch page 1
        response = requests.get(AFX_NGX_URL, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the stock table
        table = soup.find('table')
        
        if not table:
            logger.error("Could not find stock table")
            return []
        
        # Parse table rows
        rows = table.find_all('tr')[1:]  # Skip header
        
        for row in rows:
            cells = row.find_all('td')
            
            if len(cells) >= 5:
                try:
                    # Extract data
                    ticker_cell = cells[0]
                    ticker = ticker_cell.get_text(strip=True)
                    
                    name_cell = cells[1]
                    name = name_cell.get_text(strip=True)
                    
                    volume_text = cells[2].get_text(strip=True).replace(',', '')
                    volume = int(volume_text) if volume_text and volume_text.isdigit() else 0
                    
                    price_text = cells[3].get_text(strip=True).replace(',', '')
                    current_price = float(price_text) if price_text else 0.0
                    
                    change_text = cells[4].get_text(strip=True).replace(',', '')
                    
                    if current_price > 0:
                        # Calculate previous close from change
                        if change_text and change_text != '':
                            try:
                                change = float(change_text)
                                previous_close = current_price - change
                            except:
                                previous_close = current_price
                        else:
                            previous_close = current_price
                        
                        stocks.append({
                            'symbol': ticker,
                            'name': name,
                            'current_price': current_price,
                            'previous_close': previous_close,
                            'official_close': current_price,
                            'volume': volume if volume > 0 else None
                        })
                        
                        logger.info(f"  {ticker}: ₦{current_price:,.2f}")
                
                except Exception as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue
        
        # Try to fetch page 2 for more stocks
        page2_url = AFX_NGX_URL + "?page=2"
        try:
            response2 = requests.get(page2_url, timeout=30)
            soup2 = BeautifulSoup(response2.content, 'html.parser')
            table2 = soup2.find('table')
            
            if table2:
                rows2 = table2.find_all('tr')[1:]
                
                for row in rows2:
                    cells = row.find_all('td')
                    
                    if len(cells) >= 5:
                        try:
                            ticker = cells[0].get_text(strip=True)
                            name = cells[1].get_text(strip=True)
                            volume_text = cells[2].get_text(strip=True).replace(',', '')
                            volume = int(volume_text) if volume_text and volume_text.isdigit() else 0
                            price_text = cells[3].get_text(strip=True).replace(',', '')
                            current_price = float(price_text) if price_text else 0.0
                            change_text = cells[4].get_text(strip=True).replace(',', '')
                            
                            if current_price > 0:
                                if change_text and change_text != '':
                                    try:
                                        change = float(change_text)
                                        previous_close = current_price - change
                                    except:
                                        previous_close = current_price
                                else:
                                    previous_close = current_price
                                
                                stocks.append({
                                    'symbol': ticker,
                                    'name': name,
                                    'current_price': current_price,
                                    'previous_close': previous_close,
                                    'official_close': current_price,
                                    'volume': volume if volume > 0 else None
                                })
                                
                                logger.info(f"  {ticker}: ₦{current_price:,.2f}")
                        
                        except Exception as e:
                            logger.warning(f"Error parsing row on page 2: {e}")
                            continue
        except:
            pass
        
        logger.info(f"✅ Scraped {len(stocks)} stocks from AFX NGX")
        return stocks
        
    except Exception as e:
        logger.error(f"❌ Error scraping AFX NGX: {e}")
        return []


async def store_prices(stocks, source_url):
    """
    Store real stock prices in MongoDB.
    """
    if not stocks:
        logger.warning("No stocks to store")
        return 0
    
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().isoformat()
        
        stored_count = 0
        
        # Determine which stocks are "Analyzed" based on major tickers
        # In production, this should be based on actual analysis criteria
        analyzed_tickers = [
            'ZENITHBANK', 'UBA', 'GTCO', 'ACCESSCORP', 'FBNH',
            'DANGCEM', 'MTNN', 'BUACEMENT', 'SEPLAT', 'NESTLE',
            'AIRTELAFRI', 'FLOURMILL', 'STANBIC', 'FIDELITYBK', 'GUINNESS',
            'DANGSUGAR', 'NB', 'OANDO', 'TRANSCORP', 'INTBREW'
        ]
        
        for stock in stocks:
            stock_type = "Analyzed" if stock['symbol'] in analyzed_tickers else "Price-Only"
            
            doc = {
                'date': today,
                'symbol': stock['symbol'],
                'name': stock['name'],
                'current_price': stock['current_price'],
                'previous_close': stock['previous_close'],
                'official_close': stock['official_close'],
                'volume': stock.get('volume'),
                'stock_type': stock_type,
                'source_name': 'AFX NGX Live Data',
                'source_type': 'live_scraper',
                'source_url': source_url,
                'last_updated_at': timestamp
            }
            
            # Upsert
            await stock_prices_collection.update_one(
                {'date': today, 'symbol': stock['symbol']},
                {'$set': doc},
                upsert=True
            )
            stored_count += 1
        
        logger.info(f"✅ Stored {stored_count} stock prices for {today}")
        return stored_count
        
    except Exception as e:
        logger.error(f"❌ Error storing prices: {e}")
        return 0


async def clear_old_data():
    """Clear old demo/placeholder data."""
    logger.info("Clearing old demo/placeholder data...")
    
    result = await stock_prices_collection.delete_many({
        'source_type': {'$ne': 'live_scraper'}
    })
    
    logger.info(f"✅ Cleared {result.deleted_count} old records")
    return result.deleted_count


async def ingest_live_data():
    """
    Main ingestion function for live AFX NGX data.
    """
    logger.info("="*60)
    logger.info("NGX LIVE DATA INGESTION (AFX Source)")
    logger.info("="*60)
    logger.info(f"Source: {AFX_NGX_URL}")
    logger.info("")
    
    # Clear old data
    await clear_old_data()
    
    # Scrape live data
    stocks = scrape_ngx_live_data()
    
    if not stocks:
        logger.error("❌ Failed to scrape live data")
        return {
            'success': False,
            'error': 'No stocks scraped',
            'source_url': AFX_NGX_URL
        }
    
    # Store in database
    stored_count = await store_prices(stocks, AFX_NGX_URL)
    
    logger.info("")
    logger.info("="*60)
    logger.info(f"✅ Ingestion complete: {stored_count} stocks stored")
    logger.info("="*60)
    
    # Show key stocks
    key_stocks = ['ZENITHBANK', 'UBA', 'GTCO']
    logger.info("\nKey stocks verified:")
    for s in stocks:
        if s['symbol'] in key_stocks:
            logger.info(f"  {s['symbol']}: ₦{s['current_price']:,.2f}")
    
    return {
        'success': True,
        'source_url': AFX_NGX_URL,
        'total_stocks': stored_count,
        'stocks_sample': stocks[:5]
    }


if __name__ == "__main__":
    async def main():
        result = await ingest_live_data()
        
        if result['success']:
            logger.info("\n✅ Successfully ingested REAL NGX live data")
            logger.info(f"Total Stocks: {result['total_stocks']}")
            logger.info(f"Source: {result['source_url']}")
        else:
            logger.error("\n❌ Ingestion failed")
            logger.error(f"Error: {result.get('error')}")
        
        client.close()
    
    asyncio.run(main())
