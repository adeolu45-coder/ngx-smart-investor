# NGX Smart Investor - Daily Performance Tracker

## Overview
The Daily Performance Tracker evaluates the performance of trading signals across multiple time horizons (1D, 3D, 5D, 10D) using existing NGX EOD price data and signal outputs.

## 📁 Data Structure

### Price Data
Location: `/app/backend/data/prices/`
Format: `{date}.json` (e.g., `2025-03-10.json`)

```json
{
  "date": "2025-03-10",
  "stocks": [
    {
      "symbol": "ZENITHBANK",
      "name": "Zenith Bank Plc",
      "close": 45.50,
      "change": 2.25,
      "percentChange": 5.2,
      "volume": 25000000,
      "category": "Analyzed Opportunities"
    }
  ]
}
```

### Signal Data
Location: `/app/backend/data/signals/`
Format: `{date}.json` (e.g., `2025-03-10.json`)

```json
{
  "date": "2025-03-10",
  "signals": [
    {
      "id": "sig_001",
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
  ]
}
```

## 🔧 Backend API Endpoints

### 1. Get Tracked Signals with Performance
**Endpoint:** `GET /api/performance/tracked-signals`

Returns all signals with calculated performance metrics.

**Response:**
```json
[
  {
    "id": "sig_001",
    "date": "2025-03-10",
    "symbol": "ZENITHBANK",
    "name": "Zenith Bank Plc",
    "signalType": "Buy Candidate",
    "entryPrice": 45.5,
    "overallScore": 8.4,
    "metrics": {
      "return1D": 2.86,
      "return3D": 6.59,
      "return5D": 5.49,
      "return10D": null,
      "result1D": "WIN",
      "result3D": "WIN",
      "result5D": "WIN",
      "result10D": null,
      "score1D": 1,
      "score3D": 1,
      "score5D": 1,
      "score10D": null
    }
  }
]
```

### 2. Get Performance Summary
**Endpoint:** `GET /api/performance/summary`

Returns aggregated performance statistics.

**Response:**
```json
{
  "totalSignals": 5,
  "winRate1D": 100.0,
  "winRate3D": 100.0,
  "winRate5D": 100.0,
  "winRate10D": 0.0,
  "avgReturn1D": 2.88,
  "avgReturn3D": 7.23,
  "avgReturn5D": 9.37,
  "avgReturn10D": 0.0,
  "bySignalType": {
    "Buy Candidate": {
      "count": 2,
      "winRate1D": 100.0,
      "avgReturn1D": 2.64
    },
    "Best Trade of the Day": {
      "count": 1,
      "winRate1D": 100.0,
      "avgReturn1D": 2.61
    }
  }
}
```

### 3. Get Signals for Date
**Endpoint:** `GET /api/signals/{date}`

Returns all signals for a specific date.

### 4. Get Prices for Date
**Endpoint:** `GET /api/prices/{date}`

Returns all stock prices for a specific date.

## 📊 Performance Calculation Logic

### Return Calculation
```python
return_pct = ((exit_price - entry_price) / entry_price) * 100
```

### Result Determination
- **WIN**: Return > +1.0%
- **LOSS**: Return < -1.0%
- **FLAT**: Return between -1.0% and +1.0%

### Score Assignment
- **WIN**: +1
- **LOSS**: -1
- **FLAT**: 0

### Time Horizons
- **1D**: Next trading day after signal
- **3D**: 3 trading days after signal
- **5D**: 5 trading days after signal
- **10D**: 10 trading days after signal

## 🎨 Dashboard Features

### Summary Cards
1. **Total Signals**: Count of tracked opportunities
2. **Win Rate (1D)**: 1-day success rate
3. **Avg Return (1D)**: Average 1-day return percentage
4. **Avg Return (5D)**: Average 5-day return percentage

### Charts
1. **Win Rate by Period**: Bar chart showing win rates across 1D, 3D, 5D, 10D
2. **Average Returns by Period**: Line chart showing return trends
3. **Performance by Signal Type**: Breakdown of count and win rate by signal category

### Tracked Signals Table
- **Period Tabs**: Switch between 1D, 3D, 5D, 10D views
- **Columns**: Date, Symbol, Type, Entry Price, Score, Return, Result
- **Color Coding**: 
  - Green badges for WIN
  - Red badges for LOSS
  - Gray badges for FLAT/N/A
  - Signal type badges (purple, green, blue, yellow)

## 🚀 Adding New Data

### To Add New Price Data:
1. Create a new file: `/app/backend/data/prices/YYYY-MM-DD.json`
2. Follow the price data format above
3. Include all tracked stocks with closing prices

### To Add New Signals:
1. Create a new file: `/app/backend/data/signals/YYYY-MM-DD.json`
2. Follow the signal data format above
3. Exclude "Avoid" signals (they won't be tracked)

### Automatic Performance Calculation:
The system automatically:
- Loads all available signal and price data
- Calculates returns for each time horizon
- Updates the dashboard in real-time
- No manual intervention required

## 📈 Signal Types

1. **Best Trade of the Day**: Highest-conviction signal (purple badge)
2. **Buy Candidate**: Strong buy recommendation (green badge)
3. **Watchlist**: Monitor for entry opportunity (blue badge)
4. **Speculative**: Higher-risk opportunity (yellow badge)
5. **Avoid**: Not tracked (red badge, excluded from performance)

## 🔄 Refresh Data

Click the **Refresh** button in the dashboard to reload the latest performance data from the backend.

## 📝 Current Implementation Status

### ✅ Completed Features:
- Full backend API with performance calculation
- Complete dashboard UI with all metrics
- Multiple time horizon tracking (1D, 3D, 5D, 10D)
- Real-time data loading from file system
- Win/Loss/Flat determination
- Signal type categorization
- Interactive period switching
- Comprehensive charts and visualizations

### 📊 Sample Data Available:
- Price data: 2025-03-10 through 2025-03-24 (partial)
- Signal data: 2025-03-10 (5 signals)
- Performance tracked for 1D, 3D, and 5D horizons

### 🎯 Current Performance (Sample Data):
- **Total Signals**: 5
- **Win Rate**: 100% (1D, 3D, 5D)
- **Avg Return (1D)**: +2.88%
- **Avg Return (3D)**: +7.23%
- **Avg Return (5D)**: +9.37%

## 🛠️ Technical Stack

**Backend:**
- FastAPI
- Python 3.x
- Motor (MongoDB async driver)
- Pydantic for data validation

**Frontend:**
- React 19
- Recharts for data visualization
- Tailwind CSS for styling
- Radix UI components
- Axios for API calls

## 📦 Environment Variables

**Backend (.env):**
```
MONGO_URL=mongodb://localhost:27017
DB_NAME=test_database
CORS_ORIGINS=*
```

**Frontend (.env):**
```
REACT_APP_BACKEND_URL=https://portfolio-alerts-1.preview.emergentagent.com
```

## 🚦 Running the Application

### Start Backend:
```bash
sudo supervisorctl restart backend
```

### Start Frontend:
```bash
sudo supervisorctl restart frontend
```

### Check Status:
```bash
sudo supervisorctl status
```

## 🧪 Testing API Endpoints

### Test Tracked Signals:
```bash
curl http://localhost:8001/api/performance/tracked-signals
```

### Test Summary:
```bash
curl http://localhost:8001/api/performance/summary
```

## 📊 Data Requirements

### For Complete Tracking:
- Continuous daily price data (no gaps)
- Signal data for days you want to track
- Minimum 1 day of future price data for 1D tracking
- Minimum 3 days for 3D tracking
- Minimum 5 days for 5D tracking
- Minimum 10 days for 10D tracking

### Missing Data Handling:
- If future price data is unavailable, metric shows as `null`
- Result shows as "N/A"
- Does not affect other time horizon calculations

## 🎯 Next Steps (If Needed)

1. **Add More Historical Data**: Populate more dates with price and signal data
2. **Date Range Filtering**: Add UI controls to filter by date range
3. **Export Functionality**: Add CSV/Excel export of performance data
4. **Advanced Analytics**: Add more sophisticated metrics (Sharpe ratio, max drawdown, etc.)
5. **Signal Comparison**: Compare performance of different signal types side-by-side
6. **Alert System**: Notify when signals reach target or stop-loss levels

## 📞 Support

For issues or questions about the Performance Tracker, refer to:
- Backend logs: `/var/log/supervisor/backend.*.log`
- Frontend logs: `/var/log/supervisor/frontend.*.log`
- Data structure validation in backend models

---

**Dashboard URL**: https://portfolio-alerts-1.preview.emergentagent.com

**Last Updated**: March 2025
