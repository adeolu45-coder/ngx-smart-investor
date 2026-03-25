from fastapi import FastAPI, APIRouter, HTTPException, Request, Depends
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from contextlib import asynccontextmanager
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone, timedelta

# Rate limiting
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Authentication
from auth_security import (
    hash_password, verify_password, create_access_token, verify_token,
    require_admin, get_current_user, AuditLogger, get_client_ip,
    check_login_rate_limit, record_login_attempt, get_remaining_lockout_time,
    validate_symbol, validate_date, security
)

# Scheduled ingestion
from scheduled_ingestion import start_scheduler, stop_scheduler, get_scheduler_status, run_ingestion_now

# Reference data layer
from reference_data import (
    fetch_reference_prices, store_reference_prices, get_reference_status,
    get_combined_prices, get_market_data_status, ReferenceDataStatus
)

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Setup rate limiter
limiter = Limiter(key_func=get_remote_address)

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Collections
stock_prices_collection = db.stock_prices
trading_signals_collection = db.trading_signals
long_term_signals_collection = db.long_term_signals
alerts_collection = db.alerts
watchlist_collection = db.user_watchlist
alert_history_collection = db.alert_history
alert_status_collection = db.alert_generation_status
audit_logs_collection = db.audit_logs
reference_prices_collection = db.reference_prices

# Audit logger
audit_logger = AuditLogger(db)


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting NGX Smart Investor API...")
    start_scheduler()
    yield
    # Shutdown
    logger.info("Shutting down NGX Smart Investor API...")
    stop_scheduler()


# Create the main app with lifespan
app = FastAPI(
    title="NGX Smart Investor API", 
    version="2.0.0",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://ngx-frontend-i8nw.onrender.com",
        "http://localhost:3000",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "NGX Smart Investor API is live"}
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")


# ============================================
# DATA MODELS
# ============================================

class StatusCheck(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    client_name: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class StatusCheckCreate(BaseModel):
    client_name: str


class StockPrice(BaseModel):
    """
    NGX Stock Price Data - CRITICAL FIELDS
    All fields must be stored separately and clearly.
    """
    date: str  # Format: YYYY-MM-DD
    symbol: str
    name: str
    current_price: float
    previous_close: float
    official_close: float
    volume: Optional[int] = None
    stock_type: str  # "Analyzed" or "Price-Only"
    
    # Calculated field (derived from current_price and previous_close)
    @property
    def change_percent(self) -> Optional[float]:
        """Calculate change percentage: (current - previous) / previous"""
        if self.previous_close and self.previous_close > 0:
            return round(((self.current_price - self.previous_close) / self.previous_close) * 100, 2)
        return None
    
    @property
    def change_amount(self) -> Optional[float]:
        """Calculate change amount: current - previous"""
        if self.previous_close:
            return round(self.current_price - self.previous_close, 2)
        return None


class TradingSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    date: str
    symbol: str
    name: str
    signalType: str  # "Best Trade of the Day", "Buy Candidate", "Watchlist", "Speculative"
    entryPrice: float
    businessScore: Optional[float] = None
    setupScore: Optional[float] = None
    dataScore: Optional[float] = None
    opportunityScore: Optional[float] = None
    overallScore: float
    reason: Optional[str] = None
    targetPrice: Optional[float] = None
    stopLoss: Optional[float] = None


class PerformanceMetrics(BaseModel):
    return1D: Optional[float] = None
    return3D: Optional[float] = None
    return5D: Optional[float] = None
    return10D: Optional[float] = None
    result1D: Optional[str] = None  # WIN/LOSS/FLAT/PENDING
    result3D: Optional[str] = None
    result5D: Optional[str] = None
    result10D: Optional[str] = None
    score1D: Optional[int] = None  # +1/0/-1
    score3D: Optional[int] = None
    score5D: Optional[int] = None
    score10D: Optional[int] = None


class TrackedSignal(BaseModel):
    id: str
    date: str
    symbol: str
    name: str
    signalType: str
    entryPrice: float
    overallScore: float
    metrics: PerformanceMetrics


class PerformanceSummary(BaseModel):
    totalSignals: int
    sufficientData: bool
    winRate1D: Optional[float] = None
    winRate3D: Optional[float] = None
    winRate5D: Optional[float] = None
    winRate10D: Optional[float] = None
    avgReturn1D: Optional[float] = None
    avgReturn3D: Optional[float] = None
    avgReturn5D: Optional[float] = None
    avgReturn10D: Optional[float] = None
    bySignalType: Dict[str, Any] = {}


class StockListItem(BaseModel):
    """Stock list item for display"""
    symbol: str
    name: str
    current_price: float
    previous_close: float
    change_percent: Optional[float]
    change_amount: Optional[float]
    stock_type: str  # "Analyzed" or "Price-Only"
    last_updated: str


class PriceIntegrityCheck(BaseModel):
    """Price data integrity validation result"""
    total_stocks: int
    analyzed_stocks: int
    price_only_stocks: int
    mismatches: List[Dict[str, Any]]
    missing_data: List[str]
    validation_passed: bool


class BulkPriceImport(BaseModel):
    prices: List[StockPrice]


class BulkSignalImport(BaseModel):
    signals: List[TradingSignal]


# ============================================
# ALERT DATA MODELS
# ============================================

class Alert(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str  # PRICE_TARGET_HIT, STOP_LOSS_HIT, SIGNAL_UPGRADE, etc.
    symbol: str
    name: str
    message: str
    details: Dict[str, Any] = {}
    priority: str = "medium"  # critical, high, medium, low
    is_read: bool = False
    is_dismissed: bool = False
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    triggered_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class WatchlistItem(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = "default"  # Default user for now
    symbol: str
    name: str
    entry_price: float
    added_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    notes: str = ""
    alert_on_target: bool = True
    alert_on_stop: bool = True
    custom_target: Optional[float] = None
    custom_stop: Optional[float] = None


class WatchlistAddRequest(BaseModel):
    symbol: str
    notes: str = ""
    custom_target: Optional[float] = None
    custom_stop: Optional[float] = None


# ============================================
# AUTHENTICATION MODELS
# ============================================

class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    access_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: Optional[int] = None
    expires_at: Optional[str] = None
    message: Optional[str] = None


# ============================================
# AUTHENTICATION ENDPOINTS
# ============================================

@api_router.post("/auth/login", response_model=LoginResponse)
@limiter.limit("5/minute")
async def login(request: Request, login_data: LoginRequest):
    """
    Admin login endpoint with rate limiting.
    Returns JWT token on success.
    """
    ip = get_client_ip(request)
    
    # Check rate limit
    if not check_login_rate_limit(ip):
        remaining = get_remaining_lockout_time(ip)
        await audit_logger.log(
            action="login_attempt",
            actor=login_data.username,
            endpoint="/api/auth/login",
            status="rate_limited",
            ip_address=ip,
            details={"reason": "Too many attempts"}
        )
        raise HTTPException(
            status_code=429,
            detail=f"Too many login attempts. Try again in {remaining} seconds."
        )
    
    # Get credentials from environment
    admin_username = os.environ.get('ADMIN_USERNAME', 'admin')
    admin_password_hash = os.environ.get('ADMIN_PASSWORD_HASH')
    
    if not admin_password_hash:
        logger.error("ADMIN_PASSWORD_HASH not configured")
        raise HTTPException(status_code=500, detail="Authentication not configured")
    
    # Verify credentials
    if login_data.username != admin_username or not verify_password(login_data.password, admin_password_hash):
        record_login_attempt(ip, success=False)
        await audit_logger.log(
            action="login_attempt",
            actor=login_data.username,
            endpoint="/api/auth/login",
            status="failed",
            ip_address=ip,
            details={"reason": "Invalid credentials"}
        )
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    # Create token
    token_data = create_access_token(login_data.username, role="admin")
    record_login_attempt(ip, success=True)
    
    await audit_logger.log(
        action="login_attempt",
        actor=login_data.username,
        endpoint="/api/auth/login",
        status="success",
        ip_address=ip
    )
    
    return LoginResponse(
        success=True,
        access_token=token_data["access_token"],
        token_type=token_data["token_type"],
        expires_in=token_data["expires_in"],
        expires_at=token_data["expires_at"]
    )


@api_router.post("/auth/logout")
async def logout(request: Request, current_user: Dict = Depends(get_current_user)):
    """Logout endpoint (token invalidation)."""
    if current_user:
        await audit_logger.log(
            action="logout",
            actor=current_user.get("username", "unknown"),
            endpoint="/api/auth/logout",
            status="success",
            ip_address=get_client_ip(request)
        )
    
    return {"success": True, "message": "Logged out successfully"}


@api_router.get("/auth/verify")
async def verify_auth(current_user: Dict = Depends(get_current_user)):
    """Verify if current token is valid."""
    if not current_user:
        return {"authenticated": False}
    
    return {
        "authenticated": True,
        "username": current_user.get("username"),
        "role": current_user.get("role")
    }


@api_router.get("/auth/me")
async def get_current_user_info(current_user: Dict = Depends(require_admin)):
    """Get current user information (requires auth)."""
    return {
        "username": current_user.get("username"),
        "role": current_user.get("role")
    }


# ============================================
# HELPER FUNCTIONS
# ============================================

async def get_price_for_date_symbol(date_str: str, symbol: str) -> Optional[float]:
    """Get closing price for a symbol on a specific date from MongoDB."""
    try:
        price_doc = await stock_prices_collection.find_one(
            {"date": date_str, "symbol": symbol},
            {"_id": 0, "current_price": 1}
        )
        return price_doc['current_price'] if price_doc else None
    except Exception as e:
        logger.error(f"Error fetching price for {symbol} on {date_str}: {e}")
        return None


def calculate_return(entry_price: float, exit_price: float) -> float:
    """Calculate percentage return."""
    return ((exit_price - entry_price) / entry_price) * 100


def determine_result(return_pct: Optional[float]) -> tuple[Optional[str], Optional[int]]:
    """Determine WIN/LOSS/FLAT based on return. Fixed logic: WIN if return > 0."""
    if return_pct is None:
        return "PENDING", None
    
    if return_pct > 0:
        return "WIN", 1
    elif return_pct < 0:
        return "LOSS", -1
    else:
        return "FLAT", 0


def get_future_date(start_date: str, days: int) -> str:
    """Get date N days after start_date."""
    date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    future_date = date_obj + timedelta(days=days)
    return future_date.strftime("%Y-%m-%d")


async def calculate_signal_performance(signal: Dict, signal_date: str) -> PerformanceMetrics:
    """Calculate performance metrics for a signal."""
    metrics = PerformanceMetrics()
    entry_price = signal['entryPrice']
    symbol = signal['symbol']
    
    # Calculate for each time period
    for days, metric_prefix in [(1, '1D'), (3, '3D'), (5, '5D'), (10, '10D')]:
        target_date = get_future_date(signal_date, days)
        exit_price = await get_price_for_date_symbol(target_date, symbol)
        
        if exit_price:
            return_pct = calculate_return(entry_price, exit_price)
            result, score = determine_result(return_pct)
            
            setattr(metrics, f'return{metric_prefix}', round(return_pct, 2))
            setattr(metrics, f'result{metric_prefix}', result)
            setattr(metrics, f'score{metric_prefix}', score)
        else:
            # Price data not available yet - mark as PENDING
            setattr(metrics, f'return{metric_prefix}', None)
            setattr(metrics, f'result{metric_prefix}', 'PENDING')
            setattr(metrics, f'score{metric_prefix}', None)
    
    return metrics


# ============================================
# API ENDPOINTS
# ============================================

@api_router.get("/")
async def root():
    return {"message": "NGX Smart Investor - Performance Tracker API"}


@api_router.post("/status", response_model=StatusCheck)
async def create_status_check(input: StatusCheckCreate):
    status_dict = input.model_dump()
    status_obj = StatusCheck(**status_dict)
    
    doc = status_obj.model_dump()
    doc['timestamp'] = doc['timestamp'].isoformat()
    
    _ = await db.status_checks.insert_one(doc)
    return status_obj


@api_router.get("/status")
async def get_status_checks():
    try:
        status_checks = await db.status_checks.find({}, {"_id": 0}).to_list(100)

        return {
            "status": "ok",
            "count": len(status_checks),
            "data": status_checks
        }

    except Exception as e:
        return {
            "error": "Failed to fetch status",
            "details": str(e)
        }


# ============================================
# DATA IMPORT ENDPOINTS
# ============================================

@api_router.post("/import/prices")
async def import_prices(data: BulkPriceImport):
    """Import stock prices in bulk."""
    try:
        if not data.prices:
            raise HTTPException(status_code=400, detail="No prices provided")
        
        # Convert to dicts and upsert
        operations = []
        for price in data.prices:
            price_dict = price.model_dump()
            operations.append({
                "filter": {"date": price_dict['date'], "symbol": price_dict['symbol']},
                "update": {"$set": price_dict},
                "upsert": True
            })
        
        # Bulk upsert
        from pymongo import UpdateOne
        bulk_ops = [UpdateOne(op['filter'], op['update'], upsert=op['upsert']) for op in operations]
        result = await stock_prices_collection.bulk_write(bulk_ops)
        
        return {
            "success": True,
            "inserted": result.upserted_count,
            "modified": result.modified_count,
            "total": len(data.prices)
        }
    except Exception as e:
        logger.error(f"Error importing prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/import/signals")
async def import_signals(data: BulkSignalImport):
    """Import trading signals in bulk."""
    try:
        if not data.signals:
            raise HTTPException(status_code=400, detail="No signals provided")
        
        # Convert to dicts and insert
        operations = []
        for signal in data.signals:
            signal_dict = signal.model_dump()
            operations.append({
                "filter": {"id": signal_dict['id']},
                "update": {"$set": signal_dict},
                "upsert": True
            })
        
        from pymongo import UpdateOne
        bulk_ops = [UpdateOne(op['filter'], op['update'], upsert=op['upsert']) for op in operations]
        result = await trading_signals_collection.bulk_write(bulk_ops)
        
        return {
            "success": True,
            "inserted": result.upserted_count,
            "modified": result.modified_count,
            "total": len(data.signals)
        }
    except Exception as e:
        logger.error(f"Error importing signals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/signals")
async def get_all_signals():
    """Get all signals from database."""
    signals = await trading_signals_collection.find({}, {"_id": 0}).to_list(1000)
    return {"total": len(signals), "signals": signals}


@api_router.get("/signals/date/{date}")
async def get_signals_for_date(date: str):
    """Get all signals for a specific date."""
    signals = await trading_signals_collection.find({"date": date}, {"_id": 0}).to_list(1000)
    return {"date": date, "total": len(signals), "signals": signals}


@api_router.get("/prices/date/{date}")
async def get_prices_for_date(date: str):
    """Get all prices for a specific date."""
    prices = await stock_prices_collection.find({"date": date}, {"_id": 0}).to_list(1000)
    return {"date": date, "total": len(prices), "prices": prices}


@api_router.get("/prices/symbol/{symbol}")
async def get_prices_for_symbol(symbol: str, limit: int = 30):
    """Get historical prices for a symbol."""
    prices = await stock_prices_collection.find(
        {"symbol": symbol}, 
        {"_id": 0}
    ).sort("date", -1).limit(limit).to_list(limit)
    return {"symbol": symbol, "total": len(prices), "prices": prices}


@api_router.get("/stocks/list")
async def get_stock_list(stock_type: Optional[str] = None):
    """
    Get list of all stocks with latest prices.
    
    Args:
        stock_type: Filter by "Analyzed" or "Price-Only" (optional)
    """
    try:
        # Get the latest date available
        latest_date_doc = await stock_prices_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest_date_doc:
            return {
                "success": False,
                "message": "No price data available",
                "stocks": [],
                "last_updated": None
            }
        
        latest_date = latest_date_doc['date']
        
        # Build query
        query = {"date": latest_date}
        if stock_type:
            query["stock_type"] = stock_type
        
        # Get stocks for latest date
        stocks = await stock_prices_collection.find(
            query,
            {"_id": 0}
        ).to_list(1000)
        
        # Convert to StockListItem format
        stock_list = []
        for stock in stocks:
            # Calculate change percent
            change_pct = None
            change_amt = None
            if stock.get('previous_close') and stock['previous_close'] > 0:
                change_amt = stock['current_price'] - stock['previous_close']
                change_pct = (change_amt / stock['previous_close']) * 100
            
            stock_list.append({
                "symbol": stock['symbol'],
                "name": stock['name'],
                "current_price": stock['current_price'],
                "previous_close": stock['previous_close'],
                "change_percent": round(change_pct, 2) if change_pct is not None else None,
                "change_amount": round(change_amt, 2) if change_amt is not None else None,
                "stock_type": stock['stock_type'],
                "last_updated": latest_date
            })
        
        # Sort by symbol
        stock_list.sort(key=lambda x: x['symbol'])
        
        return {
            "success": True,
            "total": len(stock_list),
            "analyzed": len([s for s in stock_list if s['stock_type'] == 'Analyzed']),
            "price_only": len([s for s in stock_list if s['stock_type'] == 'Price-Only']),
            "last_updated": latest_date,
            "stocks": stock_list
        }
        
    except Exception as e:
        logger.error(f"Error fetching stock list: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/monitoring/price-integrity")
async def check_price_integrity():
    """
    Validate price data integrity.
    
    Checks:
    - current_price vs previous_close vs calculated change
    - Missing required fields
    - Data consistency
    """
    try:
        # Get latest date
        latest_date_doc = await stock_prices_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest_date_doc:
            return PriceIntegrityCheck(
                total_stocks=0,
                analyzed_stocks=0,
                price_only_stocks=0,
                mismatches=[],
                missing_data=["No price data available in database"],
                validation_passed=False
            )
        
        latest_date = latest_date_doc['date']
        
        # Get all stocks for latest date
        stocks = await stock_prices_collection.find(
            {"date": latest_date},
            {"_id": 0}
        ).to_list(1000)
        
        mismatches = []
        missing_data = []
        
        analyzed_count = 0
        price_only_count = 0
        
        for stock in stocks:
            symbol = stock['symbol']
            
            # Count by type
            if stock.get('stock_type') == 'Analyzed':
                analyzed_count += 1
            elif stock.get('stock_type') == 'Price-Only':
                price_only_count += 1
            
            # Check required fields
            required_fields = ['current_price', 'previous_close', 'official_close']
            for field in required_fields:
                if field not in stock or stock[field] is None:
                    missing_data.append(f"{symbol}: Missing {field}")
            
            # Validate change calculation
            if 'current_price' in stock and 'previous_close' in stock:
                if stock['previous_close'] and stock['previous_close'] > 0:
                    calculated_change_pct = ((stock['current_price'] - stock['previous_close']) / stock['previous_close']) * 100
                    
                    # Check if there's a stored percentChange that doesn't match
                    if 'percentChange' in stock and stock['percentChange'] is not None:
                        stored_change = stock['percentChange']
                        diff = abs(calculated_change_pct - stored_change)
                        
                        if diff > 0.01:  # Allow 0.01% tolerance for rounding
                            mismatches.append({
                                "symbol": symbol,
                                "issue": "Change % mismatch",
                                "current_price": stock['current_price'],
                                "previous_close": stock['previous_close'],
                                "calculated_change": round(calculated_change_pct, 2),
                                "stored_change": stored_change,
                                "difference": round(diff, 2)
                            })
        
        validation_passed = len(mismatches) == 0 and len(missing_data) == 0
        
        return PriceIntegrityCheck(
            total_stocks=len(stocks),
            analyzed_stocks=analyzed_count,
            price_only_stocks=price_only_count,
            mismatches=mismatches,
            missing_data=missing_data,
            validation_passed=validation_passed
        )
        
    except Exception as e:
        logger.error(f"Error checking price integrity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# PERFORMANCE TRACKING ENDPOINTS
# ============================================

@api_router.get("/performance/tracked-signals", response_model=List[TrackedSignal])
async def get_tracked_signals():
    """Get all tracked signals with performance metrics."""
    tracked_signals = []
    
    # Get all signals from database, excluding "Avoid" type
    signals = await trading_signals_collection.find(
        {"signalType": {"$ne": "Avoid"}},
        {"_id": 0}
    ).sort("date", -1).to_list(1000)
    
    if not signals:
        return []
    
    for signal in signals:
        date_str = signal['date']
        metrics = await calculate_signal_performance(signal, date_str)
        
        tracked_signal = TrackedSignal(
            id=signal['id'],
            date=date_str,
            symbol=signal['symbol'],
            name=signal['name'],
            signalType=signal['signalType'],
            entryPrice=signal['entryPrice'],
            overallScore=signal.get('overallScore', 0.0),
            metrics=metrics
        )
        tracked_signals.append(tracked_signal)
    
    return tracked_signals


@api_router.get("/performance/summary", response_model=PerformanceSummary)
async def get_performance_summary():
    """Get aggregated performance summary."""
    tracked_signals = await get_tracked_signals()
    
    total = len(tracked_signals)
    sufficient_data = total >= 20
    
    # If insufficient data, return early with warning
    if not sufficient_data:
        return PerformanceSummary(
            totalSignals=total,
            sufficientData=False,
            winRate1D=None,
            winRate3D=None,
            winRate5D=None,
            winRate10D=None,
            avgReturn1D=None,
            avgReturn3D=None,
            avgReturn5D=None,
            avgReturn10D=None,
            bySignalType={}
        )
    
    # Calculate overall metrics
    def calc_win_rate(period: str):
        wins = sum(1 for s in tracked_signals if getattr(s.metrics, f'result{period}') == 'WIN')
        counted = sum(1 for s in tracked_signals 
                     if getattr(s.metrics, f'result{period}') not in [None, 'PENDING'])
        return round((wins / counted * 100), 2) if counted > 0 else None
    
    def calc_avg_return(period: str):
        returns = [getattr(s.metrics, f'return{period}') for s in tracked_signals 
                   if getattr(s.metrics, f'return{period}') is not None]
        return round(sum(returns) / len(returns), 2) if returns else None
    
    # Calculate by signal type
    signal_types = {}
    for signal in tracked_signals:
        sig_type = signal.signalType
        if sig_type not in signal_types:
            signal_types[sig_type] = []
        signal_types[sig_type].append(signal)
    
    by_signal_type = {}
    for sig_type, signals in signal_types.items():
        by_signal_type[sig_type] = {
            "count": len(signals),
            "winRate1D": calc_win_rate_for_list(signals, '1D'),
            "avgReturn1D": calc_avg_return_for_list(signals, '1D')
        }
    
    return PerformanceSummary(
        totalSignals=total,
        sufficientData=True,
        winRate1D=calc_win_rate('1D'),
        winRate3D=calc_win_rate('3D'),
        winRate5D=calc_win_rate('5D'),
        winRate10D=calc_win_rate('10D'),
        avgReturn1D=calc_avg_return('1D'),
        avgReturn3D=calc_avg_return('3D'),
        avgReturn5D=calc_avg_return('5D'),
        avgReturn10D=calc_avg_return('10D'),
        bySignalType=by_signal_type
    )


def calc_win_rate_for_list(signals: List[TrackedSignal], period: str) -> Optional[float]:
    wins = sum(1 for s in signals if getattr(s.metrics, f'result{period}') == 'WIN')
    counted = sum(1 for s in signals 
                 if getattr(s.metrics, f'result{period}') not in [None, 'PENDING'])
    return round((wins / counted * 100), 2) if counted > 0 else None


def calc_avg_return_for_list(signals: List[TrackedSignal], period: str) -> Optional[float]:
    returns = [getattr(s.metrics, f'return{period}') for s in signals 
               if getattr(s.metrics, f'return{period}') is not None]
    return round(sum(returns) / len(returns), 2) if returns else None


@api_router.get("/monitoring/price-source-status")
async def check_price_source_status():
    """
    Get information about the current price data source.
    """
    try:
        # Get latest price record
        latest_price = await stock_prices_collection.find_one(
            {},
            {"_id": 0},
            sort=[("last_updated_at", -1)]
        )
        
        if not latest_price:
            return {
                "status": "no_data",
                "message": "No price data available in database",
                "trade_date": None,
                "source_name": None,
                "total_stocks": 0
            }
        
        # Count stocks for this date
        trade_date = latest_price['date']
        total_stocks = await stock_prices_collection.count_documents({"date": trade_date})
        
        return {
            "status": "active",
            "trade_date": trade_date,
            "source_name": latest_price.get('source_name', 'Unknown'),
            "source_type": latest_price.get('source_type', 'Unknown'),
            "source_url": latest_price.get('source_url'),
            "total_stocks": total_stocks,
            "last_updated_at": latest_price.get('last_updated_at'),
            "sample_stock": {
                "symbol": latest_price['symbol'],
                "current_price": latest_price['current_price'],
                "previous_close": latest_price['previous_close']
            }
        }
        
    except Exception as e:
        logger.error(f"Error checking price source status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Nigerian public holidays for stale data calculation
NIGERIAN_HOLIDAYS = [
    "2024-01-01", "2024-03-29", "2024-04-01", "2024-05-01", "2024-05-27",
    "2024-06-12", "2024-10-01", "2024-12-25", "2024-12-26",
    "2025-01-01", "2025-04-18", "2025-04-21", "2025-05-01", "2025-05-27",
    "2025-06-12", "2025-10-01", "2025-12-25", "2025-12-26",
]


def is_trading_day(date_obj) -> bool:
    """Check if a given date is a trading day."""
    if date_obj.weekday() >= 5:
        return False
    date_str = date_obj.strftime("%Y-%m-%d")
    return date_str not in NIGERIAN_HOLIDAYS


@api_router.get("/monitoring/ingestion-status")
async def get_ingestion_status():
    """
    Get comprehensive data ingestion status and freshness information.
    
    Returns detailed status including:
        - latest_trade_date: Most recent data in database
        - today_date: Current server date
        - server_timezone: Server timezone
        - last_successful_update: Timestamp of last successful ingestion
        - last_attempted_run: Timestamp of last ingestion attempt
        - last_attempted_trade_date: What date was attempted
        - total_stocks_loaded_for_latest_date: Stock count for latest date
        - status: 'updated' | 'stale' | 'no_new_data' | 'failed' | 'no_data'
        - is_stale: True if data is older than expected (>2 trading days)
        - stale_reason: Explanation of why data is considered stale
    """
    try:
        import pytz
        
        # Get server time info
        now_utc = datetime.now(timezone.utc)
        server_timezone = "UTC"
        today_date = now_utc.strftime("%Y-%m-%d")
        
        # WAT (West Africa Time) is UTC+1
        wat_offset = timedelta(hours=1)
        now_wat = now_utc + wat_offset
        today_wat = now_wat.strftime("%Y-%m-%d")
        
        # Get ingestion status from dedicated collection
        ingestion_status_collection = db.ingestion_status
        status_doc = await ingestion_status_collection.find_one(
            {"_id": "ingestion_status"},
            {"_id": 0}
        )
        
        # Get latest price date from actual data
        latest_price = await stock_prices_collection.find_one(
            {},
            {"_id": 0, "date": 1, "last_updated_at": 1},
            sort=[("date", -1)]
        )
        
        if not latest_price:
            return {
                "success": True,
                "latest_trade_date": None,
                "today_date": today_date,
                "today_wat": today_wat,
                "server_timezone": server_timezone,
                "last_successful_update": None,
                "last_attempted_run": status_doc.get('last_check_time') if status_doc else None,
                "last_attempted_trade_date": status_doc.get('expected_date') if status_doc else None,
                "total_stocks_loaded_for_latest_date": 0,
                "status": "no_data",
                "is_stale": True,
                "stale_reason": "No price data available in database"
            }
        
        latest_trade_date = latest_price['date']
        last_updated_at = latest_price.get('last_updated_at')
        
        # Parse latest trade date
        latest_trade_date_obj = datetime.strptime(latest_trade_date, "%Y-%m-%d")
        
        # Calculate trading days difference from today (WAT)
        today_obj = datetime.strptime(today_wat, "%Y-%m-%d")
        
        trading_days_behind = 0
        check_date = today_obj
        while check_date.date() > latest_trade_date_obj.date() and trading_days_behind < 15:
            if is_trading_day(check_date):
                trading_days_behind += 1
            check_date = check_date - timedelta(days=1)
        
        # Data is stale if more than 2 trading days behind
        is_stale = trading_days_behind > 2
        
        # Build stale reason
        if is_stale:
            stale_reason = f"Data is {trading_days_behind} trading days behind. Latest data: {latest_trade_date}, Today (WAT): {today_wat}"
        elif trading_days_behind > 0:
            stale_reason = f"Data is {trading_days_behind} trading day(s) behind but within acceptable range"
        else:
            stale_reason = None
        
        # Count total stocks for latest date
        total_stocks = await stock_prices_collection.count_documents({"date": latest_trade_date})
        
        # Determine status based on freshness
        if is_stale:
            status = "stale"
        elif status_doc:
            status = status_doc.get('status', 'unknown')
        else:
            status = "updated"
        
        return {
            "success": True,
            "latest_trade_date": latest_trade_date,
            "today_date": today_date,
            "today_wat": today_wat,
            "server_timezone": server_timezone,
            "last_successful_update": status_doc.get('last_successful_update') if status_doc else last_updated_at,
            "last_attempted_run": status_doc.get('last_check_time') if status_doc else None,
            "last_attempted_trade_date": status_doc.get('expected_date') if status_doc else None,
            "total_stocks_loaded_for_latest_date": total_stocks,
            "status": status,
            "is_stale": is_stale,
            "trading_days_behind": trading_days_behind,
            "stale_reason": stale_reason,
            "message": status_doc.get('message') if status_doc else f"Data from {latest_trade_date}"
        }
        
    except Exception as e:
        logger.error(f"Error checking ingestion status: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/monitoring/trigger-ingestion")
async def trigger_manual_ingestion(request: Request, admin: Dict = Depends(require_admin)):
    """
    PROTECTED: Manually trigger NGX data ingestion.
    Requires admin authentication.
    """
    try:
        await audit_logger.log(
            action="manual_ingestion",
            actor=admin.get("username"),
            endpoint="/api/monitoring/trigger-ingestion",
            status="started",
            ip_address=get_client_ip(request)
        )
        
        # Use the scheduled ingestion module
        result = await run_ingestion_now()
        
        await audit_logger.log(
            action="manual_ingestion",
            actor=admin.get("username"),
            endpoint="/api/monitoring/trigger-ingestion",
            status="completed" if result.get('success') else "failed",
            ip_address=get_client_ip(request),
            details=result
        )
        
        return {"success": True, **result}
        
    except Exception as e:
        logger.error(f"Manual ingestion error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/monitoring/scheduler-status")
async def get_scheduler_status_endpoint():
    """
    Get the current status of the automatic ingestion scheduler.
    Shows scheduled jobs and next run times.
    """
    try:
        status = get_scheduler_status()
        return {
            "success": True,
            **status
        }
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# REFERENCE DATA ENDPOINTS
# ============================================================

@api_router.get("/reference/prices")
async def get_reference_prices_endpoint():
    """
    Get the latest reference/live prices from secondary data source.
    This is for monitoring purposes only - official NGX data remains source of truth.
    """
    try:
        # Fetch fresh reference data
        result = await fetch_reference_prices()
        
        if result["success"]:
            # Store in database
            await store_reference_prices(db, result)
        
        return result
        
    except Exception as e:
        logger.error(f"Error fetching reference prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/reference/status")
async def get_reference_status_endpoint():
    """
    Get the current status of reference data availability.
    """
    try:
        status = await get_reference_status(db)
        return {
            "success": True,
            **status
        }
    except Exception as e:
        logger.error(f"Error getting reference status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/reference/refresh")
async def refresh_reference_prices():
    """
    Manually refresh reference prices from secondary source.
    """
    try:
        # Fetch fresh reference data
        result = await fetch_reference_prices()
        
        if result["success"]:
            # Store in database
            store_result = await store_reference_prices(db, result)
            return {
                "success": True,
                "fetched": result.get("total", 0),
                "stored": store_result.get("stored", 0),
                "source": result.get("source"),
                "timestamp": result.get("timestamp"),
                "status": result.get("status")
            }
        else:
            return {
                "success": False,
                "error": result.get("error"),
                "source": result.get("source"),
                "status": result.get("status")
            }
            
    except Exception as e:
        logger.error(f"Error refreshing reference prices: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/monitoring/price-comparison")
async def get_price_comparison(symbol: Optional[str] = None):
    """
    Compare official NGX prices with reference/live prices.
    
    Returns for each symbol:
    - symbol
    - official_close (from NGX PDF - source of truth)
    - reference_price (from live source - monitoring only)
    - difference
    - official_trade_date
    - reference_timestamp
    - official_source
    - reference_source
    - official_status
    - reference_status
    """
    try:
        symbols = [symbol] if symbol else None
        
        comparison = await get_combined_prices(db, symbols)
        
        # Get overall status
        ingestion_status = await db.ingestion_status.find_one(
            {"_id": "ingestion_status"},
            {"_id": 0}
        )
        ref_status = await get_reference_status(db)
        
        is_official_stale = ingestion_status.get("is_stale", True) if ingestion_status else True
        is_ref_available = ref_status.get("available", False)
        is_ref_live = ref_status.get("status") == ReferenceDataStatus.LIVE
        
        market_status = get_market_data_status(is_official_stale, is_ref_available, is_ref_live)
        
        return {
            "success": True,
            "comparison": comparison,
            "total": len(comparison),
            "market_data_status": market_status,
            "official_status": {
                "trade_date": ingestion_status.get("trade_date") if ingestion_status else None,
                "is_stale": is_official_stale,
                "last_update": ingestion_status.get("last_successful_update") if ingestion_status else None
            },
            "reference_status": {
                "available": is_ref_available,
                "source": ref_status.get("source"),
                "last_update": ref_status.get("last_update"),
                "is_live": is_ref_live
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting price comparison: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/monitoring/market-data-status")
async def get_market_data_status_endpoint():
    """
    Get the overall market data status indicator.
    
    Returns:
    - 🟢 Live Reference Updating
    - 🟡 Awaiting Official NGX Close
    - 🔴 Stale Official Data
    - ⚪ Official Only
    """
    try:
        # Get official status
        ingestion_status = await db.ingestion_status.find_one(
            {"_id": "ingestion_status"},
            {"_id": 0}
        )
        
        # Get reference status
        ref_status = await get_reference_status(db)
        
        is_official_stale = ingestion_status.get("is_stale", True) if ingestion_status else True
        is_ref_available = ref_status.get("available", False)
        is_ref_live = ref_status.get("status") == ReferenceDataStatus.LIVE
        
        market_status = get_market_data_status(is_official_stale, is_ref_available, is_ref_live)
        
        return {
            "success": True,
            **market_status,
            "official": {
                "trade_date": ingestion_status.get("trade_date") if ingestion_status else None,
                "is_stale": is_official_stale,
                "stale_reason": ingestion_status.get("stale_reason") if ingestion_status else None
            },
            "reference": {
                "available": is_ref_available,
                "source": ref_status.get("source"),
                "is_live": is_ref_live,
                "last_update": ref_status.get("last_update")
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting market data status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/signals/best-trade")
async def get_best_trade():
    """Get the Best Trade of the Day (highest ranked signal)."""
    # Get latest date
    latest_signal = await trading_signals_collection.find_one(
        {},
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    if not latest_signal:
        raise HTTPException(status_code=404, detail="No signals available")
    
    latest_date = latest_signal['date']
    
    # Get all Buy Candidate signals for latest date, sorted by score
    signals = await trading_signals_collection.find(
        {
            "date": latest_date,
            "signalType": "Buy Candidate"
        },
        {"_id": 0}
    ).sort("opportunityScore", -1).limit(1).to_list(1)
    
    if not signals:
        # If no Buy Candidate, try any signal with high score
        signals = await trading_signals_collection.find(
            {"date": latest_date},
            {"_id": 0}
        ).sort("opportunityScore", -1).limit(1).to_list(1)
    
    return signals[0] if signals else None


@api_router.get("/signals/top-opportunities")
async def get_top_opportunities(limit: int = 10):
    """Get top opportunities (Buy Candidate and Watchlist signals)."""
    # Get latest date
    latest_signal = await trading_signals_collection.find_one(
        {},
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    if not latest_signal:
        return {"total": 0, "opportunities": []}
    
    latest_date = latest_signal['date']
    
    # Get Buy Candidate and Watchlist signals
    signals = await trading_signals_collection.find(
        {
            "date": latest_date,
            "signalType": {"$in": ["Buy Candidate", "Watchlist"]}
        },
        {"_id": 0}
    ).sort("opportunityScore", -1).limit(limit).to_list(limit)
    
    return {"total": len(signals), "date": latest_date, "opportunities": signals}


@api_router.get("/signals/diagnostics")
async def get_signal_diagnostics():
    """
    Get signal generation diagnostics with ONE primary signal per stock.
    
    Returns:
    - Total analyzed stocks
    - Total primary signals (must equal analyzed stocks)
    - Signal counts by category
    - Best Trade of the Day (highest Buy Candidate)
    - Threshold rules used
    """
    try:
        # Get latest date
        latest_signal = await trading_signals_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest_signal:
            return {
                "status": "no_signals",
                "message": "No signals generated yet"
            }
        
        latest_date = latest_signal['date']
        
        # Count analyzed stocks
        analyzed_count = await stock_prices_collection.count_documents({
            "date": latest_date,
            "stock_type": "Analyzed"
        })
        
        # Get all signals for latest date (should be one per stock)
        signals = await trading_signals_collection.find(
            {"date": latest_date},
            {"_id": 0}
        ).to_list(1000)
        
        # Count by signal type
        signal_counts = {}
        for sig in signals:
            sig_type = sig['signalType']
            signal_counts[sig_type] = signal_counts.get(sig_type, 0) + 1
        
        # Get Best Trade (highest scoring Buy Candidate)
        buy_candidates = [s for s in signals if s['signalType'] == 'Buy Candidate']
        if buy_candidates:
            best_trade = max(buy_candidates, key=lambda x: x['opportunityScore'])
            best_trade_symbol = best_trade['symbol']
            best_trade_score = best_trade['opportunityScore']
        else:
            best_trade_symbol = None
            best_trade_score = None
        
        # Get top 5 signals
        top_signals = sorted(signals, key=lambda x: x['opportunityScore'], reverse=True)[:5]
        
        # Verify totals add up
        category_total = sum(signal_counts.values())
        
        return {
            "status": "active",
            "trade_date": latest_date,
            "total_analyzed_stocks": analyzed_count,
            "total_primary_signals": len(signals),
            "count_buy_candidate": signal_counts.get("Buy Candidate", 0),
            "count_watchlist": signal_counts.get("Watchlist", 0),
            "count_speculative": signal_counts.get("Speculative", 0),
            "count_avoid": signal_counts.get("Avoid", 0),
            "best_trade_symbol": best_trade_symbol,
            "best_trade_score": best_trade_score,
            "threshold_rules_used": {
                "buy_candidate": "Opportunity Score ≥ 7.5 AND Business Score ≥ 7",
                "watchlist": "Opportunity Score ≥ 6.0",
                "speculative": "Opportunity Score ≥ 4.5 AND Setup Score ≥ 5",
                "avoid": "Below thresholds",
                "best_trade_selection": "Highest scoring Buy Candidate (no fixed threshold)"
            },
            "totals_verified": {
                "signals_equal_stocks": len(signals) == analyzed_count,
                "categories_sum_correct": category_total == len(signals),
                "math_check": f"{signal_counts.get('Buy Candidate', 0)} + {signal_counts.get('Watchlist', 0)} + {signal_counts.get('Speculative', 0)} + {signal_counts.get('Avoid', 0)} = {category_total}"
            },
            "top_ranked": [
                {
                    "symbol": s['symbol'],
                    "signal_type": s['signalType'],
                    "score": s['opportunityScore'],
                    "confidence": s.get('confidenceLevel', 'N/A'),
                    "reason": s['reason']
                }
                for s in top_signals
            ]
        }
        
    except Exception as e:
        logger.error(f"Error in signal diagnostics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# ALERT ENDPOINTS
# ============================================

@api_router.get("/alerts/active")
async def get_active_alerts(
    limit: int = 50,
    category: Optional[str] = None
):
    """Get active (unread/undismissed) alerts with optional category filter."""
    try:
        query = {"is_dismissed": False}
        if category:
            query["category"] = category
        
        alerts = await alerts_collection.find(
            query,
            {"_id": 0}
        ).sort("created_at", -1).limit(limit).to_list(limit)
        
        # Count by priority
        priority_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        unread_count = 0
        type_counts = {}
        cat_counts = {"signal": 0, "price": 0}
        
        for alert in alerts:
            priority = alert.get("priority", "medium")
            if priority in priority_counts:
                priority_counts[priority] += 1
            if not alert.get("is_read"):
                unread_count += 1
            
            # Count by type
            t = alert.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
            
            # Count by category
            c = alert.get("category", "signal")
            if c in cat_counts:
                cat_counts[c] += 1
        
        return {
            "success": True,
            "total": len(alerts),
            "unread": unread_count,
            "by_priority": priority_counts,
            "by_type": type_counts,
            "by_category": cat_counts,
            "alerts": alerts
        }
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/alerts/history")
async def get_alert_history(
    days: int = 7, 
    limit: int = 100,
    category: Optional[str] = None,
    alert_type: Optional[str] = None,
    status: Optional[str] = None
):
    """Get alert history with filtering options."""
    try:
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        
        # Build filter
        query = {"created_at": {"$gte": cutoff_date}}
        
        if category:
            query["category"] = category
        if alert_type:
            query["type"] = alert_type
        if status == "dismissed":
            query["is_dismissed"] = True
        elif status == "active":
            query["is_dismissed"] = False
        
        alerts = await alerts_collection.find(
            query,
            {"_id": 0}
        ).sort("created_at", -1).limit(limit).to_list(limit)
        
        # Group by date
        by_date = {}
        for alert in alerts:
            date_str = alert.get("trade_date", alert.get("created_at", "")[:10])
            if date_str not in by_date:
                by_date[date_str] = []
            by_date[date_str].append(alert)
        
        # Count by type
        type_counts = {}
        for alert in alerts:
            t = alert.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        
        # Count by category
        cat_counts = {"signal": 0, "price": 0}
        for alert in alerts:
            c = alert.get("category", "signal")
            if c in cat_counts:
                cat_counts[c] += 1
        
        return {
            "success": True,
            "total": len(alerts),
            "days_covered": days,
            "filters_applied": {
                "category": category,
                "alert_type": alert_type,
                "status": status
            },
            "by_type": type_counts,
            "by_category": cat_counts,
            "by_date": by_date,
            "alerts": alerts
        }
    except Exception as e:
        logger.error(f"Error fetching alert history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/alerts/{alert_id}/read")
async def mark_alert_read(alert_id: str):
    """Mark an alert as read."""
    try:
        result = await alerts_collection.update_one(
            {"id": alert_id},
            {"$set": {"is_read": True}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        return {"success": True, "message": "Alert marked as read"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking alert read: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/alerts/{alert_id}/dismiss")
async def dismiss_alert(alert_id: str):
    """Dismiss an alert."""
    try:
        result = await alerts_collection.update_one(
            {"id": alert_id},
            {"$set": {"is_dismissed": True, "is_read": True}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        return {"success": True, "message": "Alert dismissed"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error dismissing alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/alerts/dismiss-all")
async def dismiss_all_alerts():
    """Dismiss all alerts."""
    try:
        result = await alerts_collection.update_many(
            {"is_dismissed": False},
            {"$set": {"is_dismissed": True, "is_read": True}}
        )
        
        return {
            "success": True,
            "message": f"Dismissed {result.modified_count} alerts"
        }
    except Exception as e:
        logger.error(f"Error dismissing all alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/alerts/generate")
async def generate_alerts_for_date(trade_date: Optional[str] = None):
    """
    Generate all alerts (signal changes + price triggers) for a date.
    This is the main alert generation endpoint.
    """
    try:
        from alert_engine import generate_all_alerts, check_watchlist_alerts
        
        # Get latest date if not specified
        if not trade_date:
            latest = await trading_signals_collection.find_one(
                {},
                {"_id": 0, "date": 1},
                sort=[("date", -1)]
            )
            if not latest:
                return {"success": False, "message": "No signals available"}
            trade_date = latest["date"]
        
        # Generate all alerts
        stats = await generate_all_alerts(
            db,
            trade_date,
            check_price_triggers=True,
            check_signal_changes=True
        )
        
        # Also check watchlist alerts
        watchlist_alerts = await check_watchlist_alerts(db, trade_date)
        watchlist_stored = 0
        for alert in watchlist_alerts:
            existing = await alerts_collection.find_one({
                "symbol": alert["symbol"],
                "type": alert["type"],
                "trade_date": trade_date,
                "details.is_watchlist": True
            })
            if not existing:
                await alerts_collection.insert_one(dict(alert))
                watchlist_stored += 1
        
        stats["watchlist_alerts"] = watchlist_stored
        stats["total_generated"] += watchlist_stored
        
        return {
            "success": True,
            **stats
        }
        
    except Exception as e:
        logger.error(f"Error generating alerts: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/alerts/status")
async def get_alert_generation_status():
    """Get alert generation status and diagnostics."""
    try:
        # Get generation status
        status = await alert_status_collection.find_one(
            {"_id": "latest"},
            {"_id": 0}
        )
        
        if not status:
            status = {
                "last_generation_time": None,
                "last_trade_date": None,
                "alerts_generated": 0
            }
        
        # Count active alerts
        active_count = await alerts_collection.count_documents({"is_dismissed": False})
        
        # Count today's alerts
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_count = await alerts_collection.count_documents({
            "trade_date": status.get("last_trade_date", today)
        })
        
        # Count by type
        type_pipeline = [
            {"$match": {"is_dismissed": False}},
            {"$group": {"_id": "$type", "count": {"$sum": 1}}}
        ]
        type_counts = {}
        async for doc in alerts_collection.aggregate(type_pipeline):
            type_counts[doc["_id"]] = doc["count"]
        
        # Count by category
        cat_pipeline = [
            {"$match": {"is_dismissed": False}},
            {"$group": {"_id": "$category", "count": {"$sum": 1}}}
        ]
        cat_counts = {"signal": 0, "price": 0}
        async for doc in alerts_collection.aggregate(cat_pipeline):
            if doc["_id"] in cat_counts:
                cat_counts[doc["_id"]] = doc["count"]
        
        return {
            "success": True,
            "last_generation_time": status.get("last_generation_time"),
            "last_trade_date_processed": status.get("last_trade_date"),
            "alerts_generated_today": today_count,
            "active_alerts_count": active_count,
            "duplicates_prevented": status.get("duplicates_prevented", 0),
            "signal_alerts": status.get("signal_alerts", 0),
            "price_alerts": status.get("price_alerts", 0),
            "by_type": type_counts,
            "by_category": cat_counts
        }
        
    except Exception as e:
        logger.error(f"Error fetching alert status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# WATCHLIST ENDPOINTS
# ============================================

@api_router.get("/watchlist")
async def get_watchlist(user_id: str = "default"):
    """Get user's watchlist with current prices and signals."""
    try:
        watchlist_items = await watchlist_collection.find(
            {"user_id": user_id},
            {"_id": 0}
        ).to_list(100)
        
        if not watchlist_items:
            return {
                "success": True,
                "total": 0,
                "items": []
            }
        
        # Get latest prices and signals for watchlist symbols
        symbols = [item["symbol"] for item in watchlist_items]
        
        # Get latest date
        latest_price = await stock_prices_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        latest_date = latest_price["date"] if latest_price else None
        
        # Get current prices
        prices = {}
        if latest_date:
            price_docs = await stock_prices_collection.find(
                {"date": latest_date, "symbol": {"$in": symbols}},
                {"_id": 0}
            ).to_list(100)
            for p in price_docs:
                prices[p["symbol"]] = p
        
        # Get current signals
        signals = {}
        if latest_date:
            signal_docs = await trading_signals_collection.find(
                {"date": latest_date, "symbol": {"$in": symbols}},
                {"_id": 0}
            ).to_list(100)
            for s in signal_docs:
                signals[s["symbol"]] = s
        
        # Enrich watchlist items
        enriched_items = []
        for item in watchlist_items:
            symbol = item["symbol"]
            price_data = prices.get(symbol, {})
            signal_data = signals.get(symbol, {})
            
            current_price = price_data.get("current_price", item["entry_price"])
            entry_price = item["entry_price"]
            
            # Calculate P&L
            pnl_amount = current_price - entry_price
            pnl_percent = (pnl_amount / entry_price * 100) if entry_price > 0 else 0
            
            enriched_items.append({
                **item,
                "current_price": current_price,
                "previous_close": price_data.get("previous_close"),
                "change_percent": price_data.get("change_percent") if "change_percent" in price_data else None,
                "pnl_amount": round(pnl_amount, 2),
                "pnl_percent": round(pnl_percent, 2),
                "signal_type": signal_data.get("signalType"),
                "opportunity_score": signal_data.get("opportunityScore"),
                "target_price": item.get("custom_target") or signal_data.get("targetPrice"),
                "stop_loss": item.get("custom_stop") or signal_data.get("stopLoss"),
                "confidence": signal_data.get("confidenceLevel")
            })
        
        return {
            "success": True,
            "total": len(enriched_items),
            "last_updated": latest_date,
            "items": enriched_items
        }
        
    except Exception as e:
        logger.error(f"Error fetching watchlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/watchlist/add")
async def add_to_watchlist(request: WatchlistAddRequest, user_id: str = "default"):
    """Add a stock to user's watchlist."""
    try:
        symbol = request.symbol.upper()
        
        # Check if already in watchlist
        existing = await watchlist_collection.find_one({
            "user_id": user_id,
            "symbol": symbol
        })
        
        if existing:
            raise HTTPException(status_code=400, detail=f"{symbol} is already in your watchlist")
        
        # Get stock info and current signal
        latest_date_doc = await stock_prices_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest_date_doc:
            raise HTTPException(status_code=404, detail="No price data available")
        
        latest_date = latest_date_doc["date"]
        
        # Get stock price
        stock = await stock_prices_collection.find_one(
            {"date": latest_date, "symbol": symbol},
            {"_id": 0}
        )
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        
        # Get signal
        signal = await trading_signals_collection.find_one(
            {"date": latest_date, "symbol": symbol},
            {"_id": 0}
        )
        
        # Create watchlist item
        watchlist_item = {
            "id": str(uuid.uuid4()),
            "user_id": user_id,
            "symbol": symbol,
            "name": stock.get("name", ""),
            "entry_price": stock.get("current_price"),
            "added_at": datetime.now(timezone.utc).isoformat(),
            "notes": request.notes,
            "alert_on_target": True,
            "alert_on_stop": True,
            "custom_target": request.custom_target,
            "custom_stop": request.custom_stop
        }
        
        await watchlist_collection.insert_one(watchlist_item)
        
        # Remove _id before returning
        watchlist_item.pop("_id", None)
        
        return {
            "success": True,
            "message": f"{symbol} added to watchlist",
            "item": {
                **watchlist_item,
                "current_price": stock.get("current_price"),
                "signal_type": signal.get("signalType") if signal else None,
                "target_price": request.custom_target or (signal.get("targetPrice") if signal else None),
                "stop_loss": request.custom_stop or (signal.get("stopLoss") if signal else None)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding to watchlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.delete("/watchlist/{item_id}")
async def remove_from_watchlist(item_id: str, user_id: str = "default"):
    """Remove a stock from user's watchlist."""
    try:
        result = await watchlist_collection.delete_one({
            "id": item_id,
            "user_id": user_id
        })
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        
        return {"success": True, "message": "Removed from watchlist"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing from watchlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.put("/watchlist/{item_id}")
async def update_watchlist_item(
    item_id: str,
    notes: Optional[str] = None,
    custom_target: Optional[float] = None,
    custom_stop: Optional[float] = None,
    alert_on_target: Optional[bool] = None,
    alert_on_stop: Optional[bool] = None,
    user_id: str = "default"
):
    """Update a watchlist item."""
    try:
        update_data = {}
        if notes is not None:
            update_data["notes"] = notes
        if custom_target is not None:
            update_data["custom_target"] = custom_target
        if custom_stop is not None:
            update_data["custom_stop"] = custom_stop
        if alert_on_target is not None:
            update_data["alert_on_target"] = alert_on_target
        if alert_on_stop is not None:
            update_data["alert_on_stop"] = alert_on_stop
        
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        
        result = await watchlist_collection.update_one(
            {"id": item_id, "user_id": user_id},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Watchlist item not found")
        
        return {"success": True, "message": "Watchlist item updated"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating watchlist item: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/alerts/summary")
async def get_alerts_summary():
    """Get a summary of alert activity."""
    try:
        # Count active alerts by type
        pipeline = [
            {"$match": {"is_dismissed": False}},
            {"$group": {
                "_id": "$type",
                "count": {"$sum": 1}
            }}
        ]
        
        type_counts = {}
        async for doc in alerts_collection.aggregate(pipeline):
            type_counts[doc["_id"]] = doc["count"]
        
        # Count by priority
        priority_pipeline = [
            {"$match": {"is_dismissed": False}},
            {"$group": {
                "_id": "$priority",
                "count": {"$sum": 1}
            }}
        ]
        
        priority_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        async for doc in alerts_collection.aggregate(priority_pipeline):
            if doc["_id"] in priority_counts:
                priority_counts[doc["_id"]] = doc["count"]
        
        # Total counts
        total_active = await alerts_collection.count_documents({"is_dismissed": False})
        total_unread = await alerts_collection.count_documents({"is_dismissed": False, "is_read": False})
        
        # Recent alerts (last 24 hours)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        recent_count = await alerts_collection.count_documents({
            "created_at": {"$gte": yesterday}
        })
        
        return {
            "success": True,
            "total_active": total_active,
            "total_unread": total_unread,
            "recent_24h": recent_count,
            "by_type": type_counts,
            "by_priority": priority_counts
        }
        
    except Exception as e:
        logger.error(f"Error fetching alert summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# LONG-TERM SIGNALS ENDPOINTS
# ============================================

@api_router.get("/long-term-signals")
async def get_long_term_signals(limit: int = 50):
    """Get all long-term investment signals."""
    try:
        # Get latest date
        latest = await long_term_signals_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest:
            return {"success": True, "signals": [], "message": "No long-term signals available"}
        
        signals = await long_term_signals_collection.find(
            {"date": latest["date"]},
            {"_id": 0}
        ).sort("long_term_score", -1).limit(limit).to_list(limit)
        
        return {
            "success": True,
            "date": latest["date"],
            "total": len(signals),
            "signals": signals
        }
    except Exception as e:
        logger.error(f"Error fetching long-term signals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/long-term-signals/{symbol}")
async def get_long_term_signal_for_symbol(symbol: str):
    """Get long-term signal for a specific stock."""
    try:
        signal = await long_term_signals_collection.find_one(
            {"symbol": symbol.upper()},
            {"_id": 0},
            sort=[("date", -1)]
        )
        
        if not signal:
            return {"success": False, "message": f"No long-term signal for {symbol}"}
        
        return {"success": True, "signal": signal}
    except Exception as e:
        logger.error(f"Error fetching long-term signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/long-term-signals/by-type/{signal_type}")
async def get_long_term_signals_by_type(signal_type: str, limit: int = 20):
    """Get long-term signals filtered by type."""
    try:
        valid_types = ["STRONG_LONG_TERM_BUY", "ACCUMULATE", "HOLD", "AVOID_LONG_TERM"]
        if signal_type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid signal type. Must be one of: {valid_types}")
        
        latest = await long_term_signals_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if not latest:
            return {"success": True, "signals": []}
        
        signals = await long_term_signals_collection.find(
            {"date": latest["date"], "signal_type": signal_type},
            {"_id": 0}
        ).sort("long_term_score", -1).limit(limit).to_list(limit)
        
        return {
            "success": True,
            "signal_type": signal_type,
            "total": len(signals),
            "signals": signals
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching long-term signals by type: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# ADMIN PROTECTED ENDPOINTS
# ============================================

@api_router.post("/admin/generate-signals")
@limiter.limit("2/minute")
async def admin_generate_signals(
    request: Request,
    trade_date: Optional[str] = None,
    admin: Dict = Depends(require_admin)
):
    """
    PROTECTED: Regenerate trading signals.
    Requires admin authentication.
    """
    try:
        from ngx_signal_generator import run_signal_generation
        
        # Get latest date if not specified
        if not trade_date:
            latest = await stock_prices_collection.find_one(
                {},
                {"_id": 0, "date": 1},
                sort=[("date", -1)]
            )
            if not latest:
                raise HTTPException(status_code=400, detail="No price data available")
            trade_date = latest["date"]
        
        await audit_logger.log(
            action="signal_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-signals",
            status="started",
            ip_address=get_client_ip(request),
            details={"trade_date": trade_date}
        )
        
        result = await run_signal_generation(db, trade_date)
        
        await audit_logger.log(
            action="signal_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-signals",
            status="completed",
            ip_address=get_client_ip(request),
            details={"trade_date": trade_date, "signals_generated": result.get("total_signals", 0)}
        )
        
        return {"success": True, **result}
        
    except Exception as e:
        await audit_logger.log(
            action="signal_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-signals",
            status="failed",
            ip_address=get_client_ip(request),
            details={"error": str(e)}
        )
        logger.error(f"Signal generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/admin/generate-long-term-signals")
@limiter.limit("2/minute")
async def admin_generate_long_term_signals(
    request: Request,
    trade_date: Optional[str] = None,
    admin: Dict = Depends(require_admin)
):
    """
    PROTECTED: Generate long-term investment signals.
    Requires admin authentication.
    """
    try:
        from long_term_signals import generate_long_term_signals
        
        # Get latest date if not specified
        if not trade_date:
            latest = await stock_prices_collection.find_one(
                {},
                {"_id": 0, "date": 1},
                sort=[("date", -1)]
            )
            if not latest:
                raise HTTPException(status_code=400, detail="No price data available")
            trade_date = latest["date"]
        
        await audit_logger.log(
            action="long_term_signal_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-long-term-signals",
            status="started",
            ip_address=get_client_ip(request),
            details={"trade_date": trade_date}
        )
        
        result = await generate_long_term_signals(db, trade_date)
        
        await audit_logger.log(
            action="long_term_signal_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-long-term-signals",
            status="completed",
            ip_address=get_client_ip(request),
            details={"trade_date": trade_date, "signals_generated": result.get("signals_generated", 0)}
        )
        
        return {"success": True, **result}
        
    except Exception as e:
        await audit_logger.log(
            action="long_term_signal_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-long-term-signals",
            status="failed",
            ip_address=get_client_ip(request),
            details={"error": str(e)}
        )
        logger.error(f"Long-term signal generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/admin/generate-alerts")
@limiter.limit("2/minute")
async def admin_generate_alerts_protected(
    request: Request,
    trade_date: Optional[str] = None,
    admin: Dict = Depends(require_admin)
):
    """
    PROTECTED: Generate alerts for a date.
    Requires admin authentication.
    """
    try:
        from alert_engine import generate_all_alerts
        
        # Get latest date if not specified
        if not trade_date:
            latest = await trading_signals_collection.find_one(
                {},
                {"_id": 0, "date": 1},
                sort=[("date", -1)]
            )
            if not latest:
                raise HTTPException(status_code=400, detail="No signals available")
            trade_date = latest["date"]
        
        await audit_logger.log(
            action="alert_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-alerts",
            status="started",
            ip_address=get_client_ip(request),
            details={"trade_date": trade_date}
        )
        
        result = await generate_all_alerts(db, trade_date)
        
        await audit_logger.log(
            action="alert_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-alerts",
            status="completed",
            ip_address=get_client_ip(request),
            details={"trade_date": trade_date, "alerts_generated": result.get("total_generated", 0)}
        )
        
        return {"success": True, **result}
        
    except Exception as e:
        await audit_logger.log(
            action="alert_generation",
            actor=admin.get("username"),
            endpoint="/api/admin/generate-alerts",
            status="failed",
            ip_address=get_client_ip(request),
            details={"error": str(e)}
        )
        logger.error(f"Alert generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/admin/backup")
@limiter.limit("1/minute")
async def admin_create_backup(
    request: Request,
    admin: Dict = Depends(require_admin)
):
    """
    PROTECTED: Create a full database backup.
    Requires admin authentication.
    """
    try:
        from backup_utils import create_full_backup
        
        await audit_logger.log(
            action="backup_create",
            actor=admin.get("username"),
            endpoint="/api/admin/backup",
            status="started",
            ip_address=get_client_ip(request)
        )
        
        result = await create_full_backup(db)
        
        await audit_logger.log(
            action="backup_create",
            actor=admin.get("username"),
            endpoint="/api/admin/backup",
            status="completed" if result["success"] else "failed",
            ip_address=get_client_ip(request),
            details={"total_documents": result.get("total_documents", 0)}
        )
        
        return result
        
    except Exception as e:
        await audit_logger.log(
            action="backup_create",
            actor=admin.get("username"),
            endpoint="/api/admin/backup",
            status="failed",
            ip_address=get_client_ip(request),
            details={"error": str(e)}
        )
        logger.error(f"Backup error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/admin/backups")
async def admin_list_backups(admin: Dict = Depends(require_admin)):
    """
    PROTECTED: List available backups.
    """
    try:
        from backup_utils import list_backups
        backups = await list_backups()
        return {"success": True, "backups": backups}
    except Exception as e:
        logger.error(f"List backups error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/admin/audit-logs")
async def admin_get_audit_logs(
    limit: int = 100,
    action_filter: Optional[str] = None,
    admin: Dict = Depends(require_admin)
):
    """
    PROTECTED: Get audit logs.
    """
    try:
        logs = await audit_logger.get_recent_logs(limit=limit, action_filter=action_filter)
        failed_logins = await audit_logger.get_failed_logins(hours=24)
        
        return {
            "success": True,
            "total": len(logs),
            "failed_logins_24h": failed_logins,
            "logs": logs
        }
    except Exception as e:
        logger.error(f"Get audit logs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/admin/status")
async def admin_system_status(admin: Dict = Depends(require_admin)):
    """
    PROTECTED: Get system status overview.
    """
    try:
        from backup_utils import get_last_backup_info, verify_database_integrity
        
        # Get last ingestion
        last_price = await stock_prices_collection.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        # Get last alert generation
        last_alert_gen = await alert_status_collection.find_one(
            {"_id": "latest"},
            {"_id": 0}
        )
        
        # Get counts
        stock_count = await stock_prices_collection.count_documents({})
        signal_count = await trading_signals_collection.count_documents({})
        lt_signal_count = await long_term_signals_collection.count_documents({})
        alert_count = await alerts_collection.count_documents({"is_dismissed": False})
        
        # Get last backup
        last_backup = await get_last_backup_info(db)
        
        # Get failed logins
        failed_logins = await audit_logger.get_failed_logins(hours=24)
        
        # Check integrity
        integrity = await verify_database_integrity(db)
        
        # Determine health
        health = "OK"
        warnings = []
        
        if not last_price:
            health = "WARNING"
            warnings.append("No price data")
        if not last_backup:
            warnings.append("No backup found")
        if failed_logins > 10:
            health = "WARNING"
            warnings.append(f"High failed login attempts: {failed_logins}")
        if not integrity["healthy"]:
            health = "WARNING"
            warnings.extend(integrity["issues"])
        
        return {
            "success": True,
            "health": health,
            "warnings": warnings,
            "last_data_ingestion": last_price["date"] if last_price else None,
            "last_alert_generation": last_alert_gen.get("last_generation_time") if last_alert_gen else None,
            "last_backup": last_backup.get("created_at") if last_backup else None,
            "counts": {
                "stock_prices": stock_count,
                "trading_signals": signal_count,
                "long_term_signals": lt_signal_count,
                "active_alerts": alert_count
            },
            "failed_logins_24h": failed_logins,
            "integrity": integrity
        }
        
    except Exception as e:
        logger.error(f"System status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/admin/create-indexes")
async def admin_create_indexes(
    request: Request,
    admin: Dict = Depends(require_admin)
):
    """
    PROTECTED: Create database indexes.
    """
    try:
        from backup_utils import create_database_indexes
        
        await audit_logger.log(
            action="create_indexes",
            actor=admin.get("username"),
            endpoint="/api/admin/create-indexes",
            status="started",
            ip_address=get_client_ip(request)
        )
        
        indexes = await create_database_indexes(db)
        
        await audit_logger.log(
            action="create_indexes",
            actor=admin.get("username"),
            endpoint="/api/admin/create-indexes",
            status="completed",
            ip_address=get_client_ip(request),
            details={"indexes_created": len(indexes)}
        )
        
        return {"success": True, "indexes_created": indexes}
        
    except Exception as e:
        logger.error(f"Create indexes error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup_event():
    """Initialize database indexes on startup."""
    try:
        from backup_utils import create_database_indexes
        await create_database_indexes(db)
        logger.info("Database indexes initialized")
    except Exception as e:
        logger.error(f"Failed to create indexes: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
