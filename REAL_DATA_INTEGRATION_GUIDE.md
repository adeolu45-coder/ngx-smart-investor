# NGX Smart Investor - Real Data Integration Guide

## ✅ FIXED ISSUES

### 1. Removed Sample/Static Data
- ❌ Deleted `/app/backend/data/prices/*.json` (sample files)
- ❌ Deleted `/app/backend/data/signals/*.json` (sample files)
- ✅ System now uses MongoDB for ALL data storage

### 2. Connected to Database
- ✅ All prices stored in `stock_prices` collection
- ✅ All signals stored in `trading_signals` collection
- ✅ No more hardcoded files

### 3. Fixed Performance Calculation Logic
**OLD (Wrong):**
- WIN = return > 1%
- LOSS = return < -1%
- FLAT = -1% to +1%

**NEW (Correct):**
- WIN = return > 0%
- LOSS = return < 0%
- FLAT = return == 0%
- PENDING = future data not available yet

### 4. Handle Pending States Properly
- ✅ Show "PENDING" badge (yellow) when future price data unavailable
- ✅ Show "Pending" in return column
- ✅ Do NOT show 0% or fake values

### 5. Insufficient Data Warning
- ✅ If total signals < 20: Show warning message
- ✅ Hide win rate % and average returns
- ✅ Display "--" instead of misleading percentages

### 6. Realistic Results
**Current Stats (with sample realistic data):**
- Total Signals: **52** (not 5)
- Win Rate (1D): **32.6%** (not 100%)
- Avg Return (1D): **-1.47%** (showing negative returns)
- Avg Return (5D): **-2.33%**
- Mix of WIN, LOSS, and PENDING signals

---

## 📊 DATA ARCHITECTURE

### MongoDB Collections

#### 1. `stock_prices`
```javascript
{
  "date": "2026-03-17",
  "symbol": "ZENITHBANK",
  "name": "Zenith Bank Plc",
  "close": 45.50,
  "change": 1.20,
  "percentChange": 2.71,
  "volume": 25000000,
  "category": "Analyzed Opportunities"
}
```

**Indexes needed:**
```javascript
db.stock_prices.createIndex({ "date": 1, "symbol": 1 }, { unique: true })
db.stock_prices.createIndex({ "symbol": 1, "date": -1 })
```

#### 2. `trading_signals`
```javascript
{
  "id": "unique_id_here",
  "date": "2026-03-17",
  "symbol": "ZENITHBANK",
  "name": "Zenith Bank Plc",
  "signalType": "Buy Candidate",
  "entryPrice": 45.50,
  "businessScore": 8.5,
  "setupScore": 7.8,
  "dataScore": 9.0,
  "opportunityScore": 8.3,
  "overallScore": 8.4,
  "reason": "Strong fundamentals with good technical setup",
  "targetPrice": 52.00,
  "stopLoss": 43.00
}
```

**Indexes needed:**
```javascript
db.trading_signals.createIndex({ "id": 1 }, { unique: true })
db.trading_signals.createIndex({ "date": -1 })
db.trading_signals.createIndex({ "symbol": 1 })
```

---

## 🔧 DATA IMPORT API

### 1. Import Stock Prices

**Endpoint:** `POST /api/import/prices`

**Payload:**
```json
{
  "prices": [
    {
      "date": "2026-03-17",
      "symbol": "ZENITHBANK",
      "name": "Zenith Bank Plc",
      "close": 45.50,
      "change": 1.20,
      "percentChange": 2.71,
      "volume": 25000000,
      "category": "Analyzed Opportunities"
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "inserted": 150,
  "modified": 0,
  "total": 150
}
```

### 2. Import Trading Signals

**Endpoint:** `POST /api/import/signals`

**Payload:**
```json
{
  "signals": [
    {
      "date": "2026-03-17",
      "symbol": "ZENITHBANK",
      "name": "Zenith Bank Plc",
      "signalType": "Buy Candidate",
      "entryPrice": 45.50,
      "businessScore": 8.5,
      "setupScore": 7.8,
      "dataScore": 9.0,
      "opportunityScore": 8.3,
      "overallScore": 8.4,
      "reason": "Strong technical setup",
      "targetPrice": 52.00,
      "stopLoss": 43.00
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "inserted": 25,
  "modified": 0,
  "total": 25
}
```

---

## 🔄 INTEGRATION WITH REAL NGX PIPELINE

### Option 1: Scheduled Import (Recommended)

Create a cron job or scheduled task:

```python
#!/usr/bin/env python3
"""Daily NGX data sync script"""

import requests
from datetime import datetime

def fetch_ngx_eod_data():
    """
    Fetch EOD data from NGX official API.
    Replace with your actual NGX API integration.
    """
    # TODO: Implement actual NGX API call
    # Example: response = requests.get("https://api.ngxgroup.com/eod", headers={...})
    pass

def fetch_generated_signals():
    """
    Fetch signals from your signal generation system.
    """
    # TODO: Implement actual signal retrieval
    pass

def sync_to_tracker():
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Get data
    prices = fetch_ngx_eod_data()
    signals = fetch_generated_signals()
    
    # Import to tracker
    requests.post("http://localhost:8001/api/import/prices", json={"prices": prices})
    requests.post("http://localhost:8001/api/import/signals", json={"signals": signals})
    
    print(f"✅ Synced data for {today}")

if __name__ == "__main__":
    sync_to_tracker()
```

**Schedule with cron:**
```bash
# Run daily at 6 PM (after market close)
0 18 * * 1-5 /usr/bin/python3 /app/backend/sync_ngx_data.py
```

### Option 2: Direct Database Integration

If your NGX pipeline already writes to MongoDB, create views:

```javascript
// MongoDB view for prices
db.createView(
  "stock_prices",
  "your_ngx_prices_collection",
  [
    {
      $project: {
        date: "$trading_date",
        symbol: "$stock_symbol",
        name: "$stock_name",
        close: "$closing_price",
        change: "$price_change",
        percentChange: "$percent_change",
        volume: "$trading_volume",
        category: "Analyzed Opportunities"
      }
    }
  ]
);
```

### Option 3: API Webhook

Set up your signal generation system to POST to the import endpoint:

```python
# In your signal generation code
def on_signal_generated(signal):
    requests.post(
        "http://localhost:8001/api/import/signals",
        json={"signals": [signal]}
    )
```

---

## 📈 DEMO DATA

A demo import script is provided at `/app/backend/import_data.py`:

```bash
cd /app/backend
python3 import_data.py
```

This generates:
- 750 price records (30 days × 25 stocks)
- 52 trading signals (realistic distribution)
- Varied win/loss results

**⚠️ Replace this with real data integration before production use.**

---

## 🚀 DEPLOYMENT CHECKLIST

### Before Going Live:

- [ ] Remove or disable `import_data.py` (demo script)
- [ ] Implement real NGX API integration
- [ ] Set up MongoDB indexes for performance
- [ ] Configure scheduled data sync (cron/scheduler)
- [ ] Test with 1-2 days of real data first
- [ ] Verify all signals have corresponding price data
- [ ] Set up monitoring/alerts for data pipeline failures

### Data Quality Checks:

- [ ] Prices update daily after market close
- [ ] No gaps in price history
- [ ] Signals stored on generation date
- [ ] Entry prices match actual market prices
- [ ] Signal types are standardized

### Performance Optimization:

- [ ] Add MongoDB indexes (see above)
- [ ] Consider caching summary calculations
- [ ] Archive old data (>1 year) if needed
- [ ] Monitor API response times

---

## 📊 EXPECTED REALISTIC METRICS

With real trading signals, expect:

- **Win Rate:** 30-60% (varies by strategy)
- **Avg Return:** -5% to +5% (realistic range)
- **Some LOSS signals:** Absolutely normal
- **PENDING signals:** For recent entries
- **Varied by signal type:** "Best Trade" should outperform "Speculative"

---

## 🔍 TROUBLESHOOTING

### Issue: All signals show PENDING
**Cause:** Missing future price data
**Fix:** Ensure price data is up-to-date and continuous

### Issue: Win rate still 100%
**Cause:** Only importing winning signals
**Fix:** Import ALL generated signals, including losses

### Issue: Insufficient data warning persists
**Cause:** Less than 20 signals in database
**Fix:** Wait for more signals to accumulate or backfill historical signals

### Issue: Returns calculated incorrectly
**Cause:** Price data and signal dates mismatch
**Fix:** Verify date formats are consistent (YYYY-MM-DD)

---

## 🎯 NEXT STEPS

1. **Integrate with Real NGX API:**
   - Get API credentials from NGX
   - Implement `fetch_ngx_eod_data()` function
   - Test data fetching

2. **Connect Signal Generation:**
   - Identify where signals are generated
   - Add POST calls to import endpoint
   - Verify signal schema matches

3. **Automate Daily Sync:**
   - Set up cron job or task scheduler
   - Add error handling and logging
   - Monitor daily execution

4. **Test with Real Data:**
   - Start with 1 week of backfill
   - Verify calculations are correct
   - Check dashboard displays properly

5. **Go Live:**
   - Switch to production database
   - Enable daily automation
   - Monitor performance

---

## 📞 SUPPORT

For issues with:
- **Data import:** Check `/var/log/supervisor/backend.*.log`
- **Performance calculations:** Review signal and price date alignment
- **Dashboard display:** Check browser console for errors

---

**Last Updated:** March 2026
**Status:** ✅ Ready for real data integration
