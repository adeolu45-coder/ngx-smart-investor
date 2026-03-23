"""
NGX Reference Data Layer
=========================
Provides fresher reference/live prices for monitoring while keeping
official NGX PDF data as the source of truth.

Data Sources (in priority order):
1. NGX Pulse API (preferred - API-based, 30s refresh)
2. AFX NGX Scraper (fallback - scraping-based)

IMPORTANT RULES:
- Reference data is SEPARATE from official data
- Never overwrites official_close
- Signals use ONLY official_close
- Reference prices are for monitoring/display only
"""

import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
import httpx

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# NGX Pulse API Configuration
NGX_PULSE_BASE_URL = "https://www.ngxpulse.ng"
NGX_PULSE_STOCKS_ENDPOINT = "/api/ngxdata/stocks"
NGX_PULSE_MARKET_STATUS_ENDPOINT = "/api/ngxdata/market-status"

# Get API key from environment (optional - may work without for basic requests)
NGX_PULSE_API_KEY = os.environ.get('NGX_PULSE_API_KEY', '')


class ReferenceDataStatus:
    """Status constants for reference data"""
    LIVE = "live"              # Data is being updated in real-time
    DELAYED = "delayed"        # Data is delayed but available
    STALE = "stale"            # Data is outdated
    UNAVAILABLE = "unavailable" # Source is not responding
    ERROR = "error"            # Error occurred during fetch


async def fetch_ngx_pulse_stocks() -> Dict[str, Any]:
    """
    Fetch all stock prices from NGX Pulse API.
    
    Returns:
        Dictionary with success status and stock data
    """
    try:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        if NGX_PULSE_API_KEY:
            headers["X-API-Key"] = NGX_PULSE_API_KEY
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # First check market status
            try:
                market_resp = await client.get(
                    f"{NGX_PULSE_BASE_URL}{NGX_PULSE_MARKET_STATUS_ENDPOINT}",
                    headers=headers
                )
                market_status = market_resp.json() if market_resp.status_code == 200 else None
            except:
                market_status = None
            
            # Fetch stocks
            response = await client.get(
                f"{NGX_PULSE_BASE_URL}{NGX_PULSE_STOCKS_ENDPOINT}",
                headers=headers
            )
            
            if response.status_code == 401:
                logger.warning("NGX Pulse API: Unauthorized - API key may be required")
                return {
                    "success": False,
                    "error": "API key required or invalid",
                    "source": "ngx_pulse_api",
                    "status": ReferenceDataStatus.UNAVAILABLE
                }
            
            if response.status_code == 429:
                logger.warning("NGX Pulse API: Rate limit exceeded")
                return {
                    "success": False,
                    "error": "Rate limit exceeded",
                    "source": "ngx_pulse_api",
                    "status": ReferenceDataStatus.DELAYED
                }
            
            if response.status_code != 200:
                logger.error(f"NGX Pulse API error: {response.status_code}")
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "source": "ngx_pulse_api",
                    "status": ReferenceDataStatus.ERROR
                }
            
            data = response.json()
            
            # Handle both direct list and wrapped response formats
            stocks_data = data
            if isinstance(data, dict):
                # NGX Pulse returns {"stocks": [...], "total": N}
                stocks_data = data.get("stocks", data.get("data", []))
            
            if not isinstance(stocks_data, list):
                logger.error(f"NGX Pulse API returned unexpected format: {type(stocks_data)}")
                return {
                    "success": False,
                    "error": "Invalid response format",
                    "source": "ngx_pulse_api",
                    "status": ReferenceDataStatus.ERROR
                }
            
            # Transform to our format
            stocks = []
            timestamp = datetime.now(timezone.utc).isoformat()
            
            for item in stocks_data:
                stocks.append({
                    "symbol": item.get("symbol", ""),
                    "name": item.get("name", ""),
                    "reference_price": item.get("current_price", 0),
                    "reference_change_percent": item.get("change_percent", 0),
                    "reference_volume": item.get("volume", 0),
                    "reference_timestamp": timestamp,
                    "reference_source": "NGX Pulse API",
                    "reference_status": ReferenceDataStatus.LIVE,
                    "sector": item.get("sector", ""),
                    "pe_ratio": item.get("pe_ratio")
                })
            
            logger.info(f"NGX Pulse API: Fetched {len(stocks)} stocks")
            
            return {
                "success": True,
                "stocks": stocks,
                "source": "ngx_pulse_api",
                "timestamp": timestamp,
                "market_status": market_status,
                "status": ReferenceDataStatus.LIVE,
                "total": len(stocks)
            }
            
    except httpx.TimeoutException:
        logger.error("NGX Pulse API: Timeout")
        return {
            "success": False,
            "error": "Request timeout",
            "source": "ngx_pulse_api",
            "status": ReferenceDataStatus.UNAVAILABLE
        }
    except Exception as e:
        logger.error(f"NGX Pulse API error: {e}")
        return {
            "success": False,
            "error": str(e),
            "source": "ngx_pulse_api",
            "status": ReferenceDataStatus.ERROR
        }


async def fetch_afx_fallback() -> Dict[str, Any]:
    """
    Fallback: Fetch reference prices from AFX NGX scraper.
    Used when NGX Pulse API is unavailable.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        
        AFX_URL = "https://afx.kwayisi.org/ngx/"
        
        logger.info(f"AFX Fallback: Fetching from {AFX_URL}")
        
        response = requests.get(AFX_URL, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        
        if not table:
            return {
                "success": False,
                "error": "No data table found",
                "source": "afx_scraper",
                "status": ReferenceDataStatus.ERROR
            }
        
        stocks = []
        timestamp = datetime.now(timezone.utc).isoformat()
        
        rows = table.find_all('tr')[1:]  # Skip header
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 5:
                try:
                    symbol = cells[0].get_text(strip=True)
                    name = cells[1].get_text(strip=True)
                    price_text = cells[3].get_text(strip=True).replace(',', '')
                    current_price = float(price_text) if price_text else 0.0
                    change_text = cells[4].get_text(strip=True).replace('%', '')
                    
                    if current_price > 0:
                        try:
                            change_pct = float(change_text) if change_text else 0.0
                        except:
                            change_pct = 0.0
                        
                        stocks.append({
                            "symbol": symbol,
                            "name": name,
                            "reference_price": current_price,
                            "reference_change_percent": change_pct,
                            "reference_volume": None,
                            "reference_timestamp": timestamp,
                            "reference_source": "AFX NGX (Fallback)",
                            "reference_status": ReferenceDataStatus.DELAYED
                        })
                except Exception:
                    continue
        
        # Try page 2
        try:
            response2 = requests.get(AFX_URL + "?page=2", timeout=30)
            soup2 = BeautifulSoup(response2.content, 'html.parser')
            table2 = soup2.find('table')
            
            if table2:
                for row in table2.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        try:
                            symbol = cells[0].get_text(strip=True)
                            name = cells[1].get_text(strip=True)
                            price_text = cells[3].get_text(strip=True).replace(',', '')
                            current_price = float(price_text) if price_text else 0.0
                            
                            if current_price > 0:
                                stocks.append({
                                    "symbol": symbol,
                                    "name": name,
                                    "reference_price": current_price,
                                    "reference_change_percent": 0.0,
                                    "reference_volume": None,
                                    "reference_timestamp": timestamp,
                                    "reference_source": "AFX NGX (Fallback)",
                                    "reference_status": ReferenceDataStatus.DELAYED
                                })
                        except:
                            continue
        except:
            pass
        
        logger.info(f"AFX Fallback: Fetched {len(stocks)} stocks")
        
        return {
            "success": True,
            "stocks": stocks,
            "source": "afx_scraper",
            "timestamp": timestamp,
            "status": ReferenceDataStatus.DELAYED,
            "total": len(stocks)
        }
        
    except Exception as e:
        logger.error(f"AFX Fallback error: {e}")
        return {
            "success": False,
            "error": str(e),
            "source": "afx_scraper",
            "status": ReferenceDataStatus.ERROR
        }


async def fetch_reference_prices() -> Dict[str, Any]:
    """
    Main function to fetch reference prices.
    Tries NGX Pulse API first, falls back to AFX scraper.
    
    Returns:
        Dictionary with reference price data and status
    """
    logger.info("="*60)
    logger.info("FETCHING REFERENCE PRICES")
    logger.info("="*60)
    
    # Try NGX Pulse API first
    result = await fetch_ngx_pulse_stocks()
    
    if result["success"]:
        logger.info(f"✅ NGX Pulse API: {result['total']} stocks fetched")
        return result
    
    # Fallback to AFX scraper
    logger.warning(f"NGX Pulse failed ({result.get('error')}), trying AFX fallback...")
    result = await fetch_afx_fallback()
    
    if result["success"]:
        logger.info(f"✅ AFX Fallback: {result['total']} stocks fetched")
        return result
    
    # Both failed
    logger.error("❌ All reference sources failed")
    return {
        "success": False,
        "error": "All reference sources unavailable",
        "source": "none",
        "status": ReferenceDataStatus.UNAVAILABLE,
        "stocks": []
    }


async def store_reference_prices(db, reference_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Store reference prices in a SEPARATE collection.
    NEVER overwrites official data.
    
    Args:
        db: MongoDB database instance
        reference_data: Data from fetch_reference_prices()
    
    Returns:
        Storage result
    """
    if not reference_data.get("success") or not reference_data.get("stocks"):
        return {
            "success": False,
            "error": "No reference data to store",
            "stored": 0
        }
    
    try:
        reference_collection = db.reference_prices
        stocks = reference_data["stocks"]
        timestamp = reference_data["timestamp"]
        source = reference_data["source"]
        
        stored_count = 0
        
        for stock in stocks:
            doc = {
                "symbol": stock["symbol"],
                "name": stock.get("name", ""),
                "reference_price": stock["reference_price"],
                "reference_change_percent": stock.get("reference_change_percent", 0),
                "reference_volume": stock.get("reference_volume"),
                "reference_timestamp": timestamp,
                "reference_source": stock.get("reference_source", source),
                "reference_status": stock.get("reference_status", ReferenceDataStatus.LIVE),
                "sector": stock.get("sector"),
                "pe_ratio": stock.get("pe_ratio"),
                "updated_at": datetime.now(timezone.utc)
            }
            
            # Upsert by symbol (only keep latest reference price per symbol)
            await reference_collection.update_one(
                {"symbol": stock["symbol"]},
                {"$set": doc},
                upsert=True
            )
            stored_count += 1
        
        # Update reference status document
        await db.reference_status.update_one(
            {"_id": "reference_status"},
            {"$set": {
                "last_update": timestamp,
                "source": source,
                "status": reference_data["status"],
                "total_stocks": stored_count,
                "market_status": reference_data.get("market_status")
            }},
            upsert=True
        )
        
        logger.info(f"✅ Stored {stored_count} reference prices")
        
        return {
            "success": True,
            "stored": stored_count,
            "source": source,
            "timestamp": timestamp
        }
        
    except Exception as e:
        logger.error(f"Error storing reference prices: {e}")
        return {
            "success": False,
            "error": str(e),
            "stored": 0
        }


async def get_reference_status(db) -> Dict[str, Any]:
    """
    Get the current status of reference data.
    """
    try:
        status_doc = await db.reference_status.find_one(
            {"_id": "reference_status"},
            {"_id": 0}
        )
        
        if not status_doc:
            return {
                "available": False,
                "status": ReferenceDataStatus.UNAVAILABLE,
                "message": "No reference data available"
            }
        
        last_update = status_doc.get("last_update")
        
        # Check if reference data is stale (more than 5 minutes old during market hours)
        if last_update:
            try:
                last_update_dt = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                age_minutes = (datetime.now(timezone.utc) - last_update_dt).total_seconds() / 60
                
                if age_minutes > 5:
                    status_doc["is_stale"] = True
                    status_doc["age_minutes"] = round(age_minutes, 1)
                else:
                    status_doc["is_stale"] = False
                    status_doc["age_minutes"] = round(age_minutes, 1)
            except:
                status_doc["is_stale"] = True
        
        return {
            "available": True,
            **status_doc
        }
        
    except Exception as e:
        logger.error(f"Error getting reference status: {e}")
        return {
            "available": False,
            "status": ReferenceDataStatus.ERROR,
            "error": str(e)
        }


async def get_combined_prices(db, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Get combined official + reference prices for comparison.
    KEEPS DATA STRICTLY SEPARATE.
    
    Args:
        db: MongoDB database instance
        symbols: Optional list of symbols to filter
    
    Returns:
        List of combined price records
    """
    try:
        # Get latest official data
        official_query = {}
        if symbols:
            official_query["symbol"] = {"$in": symbols}
        
        # Find the latest official trade date
        latest_official = await db.stock_prices.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest_official:
            return []
        
        official_date = latest_official["date"]
        official_query["date"] = official_date
        
        # Get official prices
        official_cursor = db.stock_prices.find(
            official_query,
            {"_id": 0}
        )
        official_prices = {doc["symbol"]: doc async for doc in official_cursor}
        
        # Get reference prices
        ref_query = {}
        if symbols:
            ref_query["symbol"] = {"$in": symbols}
        
        ref_cursor = db.reference_prices.find(
            ref_query,
            {"_id": 0}
        )
        ref_prices = {doc["symbol"]: doc async for doc in ref_cursor}
        
        # Combine (but keep separate)
        combined = []
        all_symbols = set(official_prices.keys()) | set(ref_prices.keys())
        
        for symbol in all_symbols:
            official = official_prices.get(symbol, {})
            reference = ref_prices.get(symbol, {})
            
            official_close = official.get("current_price") or official.get("official_close")
            ref_price = reference.get("reference_price")
            
            # Calculate difference if both available
            difference = None
            difference_pct = None
            if official_close and ref_price:
                difference = round(ref_price - official_close, 2)
                if official_close > 0:
                    difference_pct = round((difference / official_close) * 100, 2)
            
            combined.append({
                "symbol": symbol,
                "name": official.get("name") or reference.get("name", ""),
                # Official data (source of truth)
                "official_close": official_close,
                "official_trade_date": official.get("date"),
                "official_source": official.get("source_name", "NGX Official PDF"),
                "official_status": "available" if official_close else "unavailable",
                # Reference data (monitoring only)
                "reference_price": ref_price,
                "reference_change_percent": reference.get("reference_change_percent"),
                "reference_timestamp": reference.get("reference_timestamp"),
                "reference_source": reference.get("reference_source"),
                "reference_status": reference.get("reference_status", ReferenceDataStatus.UNAVAILABLE),
                # Comparison
                "difference": difference,
                "difference_percent": difference_pct
            })
        
        return sorted(combined, key=lambda x: x["symbol"])
        
    except Exception as e:
        logger.error(f"Error getting combined prices: {e}")
        return []


def get_market_data_status(official_stale: bool, reference_available: bool, reference_live: bool) -> Dict[str, Any]:
    """
    Determine the overall market data status indicator.
    
    Returns:
        Status indicator with emoji, label, and description
    """
    if reference_live and not official_stale:
        return {
            "indicator": "🟢",
            "label": "Live Reference Updating",
            "description": "Reference prices updating, official data current",
            "code": "live"
        }
    elif reference_live and official_stale:
        return {
            "indicator": "🟡",
            "label": "Awaiting Official NGX Close",
            "description": "Reference prices available, official data pending",
            "code": "awaiting"
        }
    elif official_stale and not reference_available:
        return {
            "indicator": "🔴",
            "label": "Stale Official Data",
            "description": "Official data outdated, no reference available",
            "code": "stale"
        }
    else:
        return {
            "indicator": "⚪",
            "label": "Official Only",
            "description": "Only official NGX data available",
            "code": "official_only"
        }


# CLI test
if __name__ == "__main__":
    async def test():
        result = await fetch_reference_prices()
        print(f"\nResult: {result['success']}")
        print(f"Source: {result.get('source')}")
        print(f"Status: {result.get('status')}")
        print(f"Total: {result.get('total', 0)}")
        
        if result.get("stocks"):
            print("\nSample stocks:")
            for stock in result["stocks"][:5]:
                print(f"  {stock['symbol']}: ₦{stock['reference_price']:,.2f}")
    
    asyncio.run(test())
