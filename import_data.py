#!/usr/bin/env python3
"""
NGX Data Import Script

This script imports real NGX stock prices and trading signals into the MongoDB database.
Replace the sample data below with actual data from your NGX pipeline.
"""

import requests
import json
from datetime import datetime, timedelta
import random

# Backend API URL
API_BASE = "http://localhost:8001/api"

def generate_realistic_ngx_data():
    """
    Generate realistic NGX stock data with CORRECT structure.
    In production, replace this with actual NGX API calls.
    """
    
    # Real NGX stocks with realistic price ranges
    # Marking some as "Analyzed" and others as "Price-Only"
    stocks = [
        # Analyzed Stocks (full data available)
        {"symbol": "ZENITHBANK", "name": "Zenith Bank Plc", "base_price": 45.0, "type": "Analyzed"},
        {"symbol": "UBA", "name": "United Bank for Africa Plc", "base_price": 28.0, "type": "Analyzed"},
        {"symbol": "GTCO", "name": "Guaranty Trust Holding Company Plc", "base_price": 52.0, "type": "Analyzed"},
        {"symbol": "ACCESSCORP", "name": "Access Holdings Plc", "base_price": 22.0, "type": "Analyzed"},
        {"symbol": "FBNH", "name": "FBN Holdings Plc", "base_price": 18.0, "type": "Analyzed"},
        {"symbol": "DANGCEM", "name": "Dangote Cement Plc", "base_price": 385.0, "type": "Analyzed"},
        {"symbol": "MTNN", "name": "MTN Nigeria Communications Plc", "base_price": 210.0, "type": "Analyzed"},
        {"symbol": "BUACEMENT", "name": "BUA Cement Plc", "base_price": 95.0, "type": "Analyzed"},
        {"symbol": "SEPLAT", "name": "Seplat Energy Plc", "base_price": 3200.0, "type": "Analyzed"},
        {"symbol": "NESTLE", "name": "Nestle Nigeria Plc", "base_price": 1850.0, "type": "Analyzed"},
        {"symbol": "AIRTELAFRI", "name": "Airtel Africa Plc", "base_price": 2100.0, "type": "Analyzed"},
        {"symbol": "FLOURMILL", "name": "Flour Mills of Nigeria Plc", "base_price": 42.0, "type": "Analyzed"},
        {"symbol": "STANBIC", "name": "Stanbic IBTC Holdings Plc", "base_price": 65.0, "type": "Analyzed"},
        {"symbol": "FIDELITYBK", "name": "Fidelity Bank Plc", "base_price": 12.5, "type": "Analyzed"},
        {"symbol": "GUINNESS", "name": "Guinness Nigeria Plc", "base_price": 55.0, "type": "Analyzed"},
        
        # Price-Only Stocks (limited data)
        {"symbol": "WAPCO", "name": "Lafarge Africa Plc", "base_price": 38.0, "type": "Price-Only"},
        {"symbol": "OANDO", "name": "Oando Plc", "base_price": 8.5, "type": "Price-Only"},
        {"symbol": "UNILEVER", "name": "Unilever Nigeria Plc", "base_price": 22.5, "type": "Price-Only"},
        {"symbol": "CADBURY", "name": "Cadbury Nigeria Plc", "base_price": 18.0, "type": "Price-Only"},
        {"symbol": "NB", "name": "Nigerian Breweries Plc", "base_price": 32.0, "type": "Price-Only"},
        {"symbol": "INTBREW", "name": "International Breweries Plc", "base_price": 6.5, "type": "Price-Only"},
        {"symbol": "TRANSCORP", "name": "Transnational Corporation Plc", "base_price": 4.2, "type": "Price-Only"},
        {"symbol": "STERLINGNG", "name": "Sterling Bank Plc", "base_price": 5.8, "type": "Price-Only"},
        {"symbol": "LIVESTOCK", "name": "Livestock Feeds Plc", "base_price": 3.5, "type": "Price-Only"},
        {"symbol": "CHAMS", "name": "Chams Holding Company Plc", "base_price": 2.1, "type": "Price-Only"},
    ]
    
    # Generate 30 days of historical data
    prices_data = []
    start_date = datetime.now() - timedelta(days=30)
    
    for day in range(30):
        current_date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
        
        for stock in stocks:
            # Store previous day's price as previous_close
            previous_price = stock["base_price"]
            
            # Simulate realistic price movements (-3% to +3% daily)
            change_pct = random.uniform(-3.0, 3.0)
            current_price = previous_price * (1 + change_pct / 100)
            
            # CRITICAL: Store all three price fields separately
            prices_data.append({
                "date": current_date,
                "symbol": stock["symbol"],
                "name": stock["name"],
                "current_price": round(current_price, 2),
                "previous_close": round(previous_price, 2),
                "official_close": round(current_price, 2),  # In real data, this comes from NGX official close
                "volume": random.randint(1000000, 50000000),
                "stock_type": stock["type"]
            })
            
            # Update base price for next day (becomes previous_close)
            stock["base_price"] = current_price
    
    return prices_data


def generate_realistic_signals():
    """
    Generate realistic trading signals with varied results.
    In production, replace with actual signal generation logic.
    
    IMPORTANT: Only generate signals for "Analyzed" stocks.
    """
    
    signal_types = ["Best Trade of the Day", "Buy Candidate", "Watchlist", "Speculative"]
    
    signals_data = []
    # ONLY Analyzed stocks - no Price-Only stocks
    signal_stocks = [
        {"symbol": "ZENITHBANK", "name": "Zenith Bank Plc", "price": 45.5, "score": 8.2},
        {"symbol": "UBA", "name": "United Bank for Africa Plc", "price": 28.75, "score": 8.5},
        {"symbol": "GTCO", "name": "Guaranty Trust Holding Company Plc", "price": 52.30, "score": 7.8},
        {"symbol": "ACCESSCORP", "name": "Access Holdings Plc", "price": 22.10, "score": 7.5},
        {"symbol": "DANGCEM", "name": "Dangote Cement Plc", "price": 385.0, "score": 8.0},
        {"symbol": "MTNN", "name": "MTN Nigeria Communications Plc", "price": 210.0, "score": 8.3},
        {"symbol": "BUACEMENT", "name": "BUA Cement Plc", "price": 95.20, "score": 7.9},
        {"symbol": "SEPLAT", "name": "Seplat Energy Plc", "price": 3250.0, "score": 7.2},
        {"symbol": "AIRTELAFRI", "name": "Airtel Africa Plc", "price": 2100.0, "score": 8.1},
        {"symbol": "FLOURMILL", "name": "Flour Mills of Nigeria Plc", "price": 42.0, "score": 7.6},
        {"symbol": "STANBIC", "name": "Stanbic IBTC Holdings Plc", "price": 65.0, "score": 8.0},
        {"symbol": "FIDELITYBK", "name": "Fidelity Bank Plc", "price": 12.5, "score": 7.8},
        {"symbol": "GUINNESS", "name": "Guinness Nigeria Plc", "price": 55.0, "score": 7.7},
        {"symbol": "NESTLE", "name": "Nestle Nigeria Plc", "price": 1850.0, "score": 8.2},
        {"symbol": "FBNH", "name": "FBN Holdings Plc", "price": 18.0, "score": 7.3},
    ]
    
    # Generate signals for the last 25 days
    start_date = datetime.now() - timedelta(days=25)
    
    for day in range(25):
        current_date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
        
        # Generate 1-3 signals per day (realistic)
        num_signals = random.randint(1, 3)
        selected_stocks = random.sample(signal_stocks, num_signals)
        
        for stock in selected_stocks:
            signal_type = random.choice(signal_types)
            
            signals_data.append({
                "date": current_date,
                "symbol": stock["symbol"],
                "name": stock["name"],
                "signalType": signal_type,
                "entryPrice": stock["price"],
                "businessScore": round(random.uniform(6.5, 9.0), 1),
                "setupScore": round(random.uniform(6.0, 9.0), 1),
                "dataScore": round(random.uniform(7.0, 9.5), 1),
                "opportunityScore": round(random.uniform(6.5, 9.0), 1),
                "overallScore": stock["score"],
                "reason": f"Strong technical setup with good fundamentals for {stock['symbol']}",
                "targetPrice": round(stock["price"] * 1.15, 2),
                "stopLoss": round(stock["price"] * 0.95, 2)
            })
    
    return signals_data


def import_data():
    """Import generated data into the database."""
    
    print("Generating realistic NGX data...")
    prices = generate_realistic_ngx_data()
    signals = generate_realistic_signals()
    
    print(f"Generated {len(prices)} price records")
    print(f"Generated {len(signals)} trading signals")
    
    # Import prices
    print("\nImporting prices...")
    try:
        response = requests.post(
            f"{API_BASE}/import/prices",
            json={"prices": prices},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        print(f"✅ Prices imported: {result['inserted']} inserted, {result['modified']} updated")
    except Exception as e:
        print(f"❌ Error importing prices: {e}")
        return False
    
    # Import signals
    print("\nImporting signals...")
    try:
        response = requests.post(
            f"{API_BASE}/import/signals",
            json={"signals": signals},
            timeout=30
        )
        response.raise_for_status()
        result = response.json()
        print(f"✅ Signals imported: {result['inserted']} inserted, {result['modified']} updated")
    except Exception as e:
        print(f"❌ Error importing signals: {e}")
        return False
    
    print("\n✅ Data import complete!")
    print("\nCheck the dashboard at: https://portfolio-alerts-1.preview.emergentagent.com")
    return True


if __name__ == "__main__":
    print("="*60)
    print("NGX Smart Investor - Data Import Script")
    print("="*60)
    print("\nThis script will populate the database with realistic data.")
    print("In production, replace this with actual NGX API integration.\n")
    
    import_data()
