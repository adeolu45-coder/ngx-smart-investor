"""
NGX Smart Investor - Long-Term Investment Signal Generator
Generates long-term investment ratings (1-2 years horizon).
SEPARATE from short-term trading signals.
"""

from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# ============================================
# LONG-TERM SIGNAL TYPES (SEPARATE FROM SHORT-TERM)
# ============================================

LONG_TERM_SIGNALS = {
    "STRONG_LONG_TERM_BUY": {
        "description": "Excellent long-term investment opportunity",
        "color": "emerald",
        "min_score": 8.0
    },
    "ACCUMULATE": {
        "description": "Good for gradual accumulation over time",
        "color": "green",
        "min_score": 6.5
    },
    "HOLD": {
        "description": "Maintain current position, neutral outlook",
        "color": "blue",
        "min_score": 4.5
    },
    "AVOID_LONG_TERM": {
        "description": "Not suitable for long-term investment",
        "color": "red",
        "min_score": 0
    }
}


def calculate_long_term_business_strength(stock: Dict[str, Any], signal: Dict[str, Any]) -> float:
    """
    Calculate business strength score (0-10).
    Factors: market position, stability, fundamentals.
    """
    score = 5.0  # Base score
    
    # Use existing business score if available
    if signal:
        business_score = signal.get("businessScore", 5.0)
        score = business_score
    
    # Large-cap stocks tend to have stronger businesses
    price = stock.get("current_price", 0)
    if price >= 500:
        score += 1.5  # Large cap bonus
    elif price >= 100:
        score += 0.8  # Mid cap bonus
    elif price < 10:
        score -= 1.0  # Penny stock penalty
    
    return min(10.0, max(0.0, score))


def calculate_financial_stability(stock: Dict[str, Any], signal: Dict[str, Any]) -> float:
    """
    Calculate financial stability score (0-10).
    More stable companies score higher.
    """
    score = 5.0
    
    # Use price as proxy for stability (established companies trade higher)
    price = stock.get("current_price", 0)
    prev_close = stock.get("previous_close", price)
    
    if price > 0 and prev_close > 0:
        volatility = abs(price - prev_close) / prev_close * 100
        
        # Lower volatility = higher stability
        if volatility < 1:
            score += 2.0
        elif volatility < 2:
            score += 1.0
        elif volatility > 5:
            score -= 1.5
    
    # Confidence level from short-term signal
    if signal:
        confidence = signal.get("confidenceLevel", "Medium")
        if confidence == "Very High":
            score += 1.5
        elif confidence == "High":
            score += 1.0
        elif confidence == "Low":
            score -= 1.0
    
    return min(10.0, max(0.0, score))


def calculate_consistency_quality(stock: Dict[str, Any], signal: Dict[str, Any]) -> float:
    """
    Calculate consistency/quality score (0-10).
    Consistent performers score higher.
    """
    score = 5.0
    
    if signal:
        # Data quality score
        data_score = signal.get("dataScore", 5.0)
        score = (score + data_score) / 2
        
        # Setup score indicates quality of trading setup
        setup_score = signal.get("setupScore", 5.0)
        score = (score + setup_score) / 2
    
    # Stock type bonus
    stock_type = stock.get("stock_type", "")
    if stock_type == "Analyzed":
        score += 0.5
    
    return min(10.0, max(0.0, score))


def calculate_trend_stability(stock: Dict[str, Any], signal: Dict[str, Any]) -> float:
    """
    Calculate long-term trend stability score (0-10).
    Stocks with stable trends score higher.
    """
    score = 5.0
    
    # Change percentage analysis
    change_pct = stock.get("change_percent")
    if change_pct is not None:
        # Moderate positive change is best for long-term
        if 0 <= change_pct <= 2:
            score += 1.5  # Steady growth
        elif -1 <= change_pct < 0:
            score += 0.5  # Minor pullback (buying opportunity)
        elif change_pct > 5:
            score -= 0.5  # Too volatile
        elif change_pct < -3:
            score -= 1.0  # Downtrend concern
    
    # Opportunity score from signal
    if signal:
        opp_score = signal.get("opportunityScore", 5.0)
        # Weight opportunity score for long-term
        score = (score * 0.6) + (opp_score * 0.4)
    
    return min(10.0, max(0.0, score))


def calculate_data_quality_lt(stock: Dict[str, Any], signal: Dict[str, Any]) -> float:
    """
    Calculate data quality score for long-term analysis (0-10).
    Better data = more reliable long-term rating.
    """
    score = 5.0
    
    # Check if we have all required data
    required_fields = ['current_price', 'previous_close', 'name', 'symbol']
    missing = sum(1 for f in required_fields if not stock.get(f))
    score -= missing * 1.5
    
    if signal:
        # Use existing data score
        data_score = signal.get("dataScore", 5.0)
        score = (score + data_score) / 2
        
        # Confidence adds to data quality
        if signal.get("confidenceLevel") in ["Very High", "High"]:
            score += 1.0
    
    return min(10.0, max(0.0, score))


def calculate_long_term_score(
    business_strength: float,
    financial_stability: float,
    consistency_quality: float,
    trend_stability: float,
    data_quality: float
) -> float:
    """
    Calculate overall long-term investment score.
    Weighted average of component scores.
    """
    weights = {
        "business_strength": 0.30,      # 30% - Most important
        "financial_stability": 0.25,    # 25% - Very important
        "consistency_quality": 0.20,    # 20% - Important
        "trend_stability": 0.15,        # 15% - Moderate
        "data_quality": 0.10            # 10% - Foundation
    }
    
    score = (
        business_strength * weights["business_strength"] +
        financial_stability * weights["financial_stability"] +
        consistency_quality * weights["consistency_quality"] +
        trend_stability * weights["trend_stability"] +
        data_quality * weights["data_quality"]
    )
    
    return round(score, 2)


def determine_long_term_signal(lt_score: float) -> str:
    """Determine long-term signal type based on score."""
    if lt_score >= 8.0:
        return "STRONG_LONG_TERM_BUY"
    elif lt_score >= 6.5:
        return "ACCUMULATE"
    elif lt_score >= 4.5:
        return "HOLD"
    else:
        return "AVOID_LONG_TERM"


def generate_long_term_reason(
    symbol: str,
    lt_signal: str,
    scores: Dict[str, float]
) -> str:
    """Generate explanation for long-term rating."""
    reasons = []
    
    if scores["business_strength"] >= 7:
        reasons.append("strong business fundamentals")
    elif scores["business_strength"] < 4:
        reasons.append("weak business position")
    
    if scores["financial_stability"] >= 7:
        reasons.append("financially stable")
    elif scores["financial_stability"] < 4:
        reasons.append("financial concerns")
    
    if scores["consistency_quality"] >= 7:
        reasons.append("consistent performance")
    
    if scores["trend_stability"] >= 7:
        reasons.append("stable long-term trend")
    elif scores["trend_stability"] < 4:
        reasons.append("trend instability")
    
    signal_desc = LONG_TERM_SIGNALS[lt_signal]["description"]
    
    if reasons:
        return f"{symbol}: {signal_desc}. Key factors: {', '.join(reasons)}."
    else:
        return f"{symbol}: {signal_desc}."


async def generate_long_term_signals(db, trade_date: str) -> Dict[str, Any]:
    """
    Generate long-term investment signals for all stocks.
    This is SEPARATE from short-term trading signals.
    """
    stats = {
        "trade_date": trade_date,
        "total_analyzed": 0,
        "signals_generated": 0,
        "by_signal": {
            "STRONG_LONG_TERM_BUY": 0,
            "ACCUMULATE": 0,
            "HOLD": 0,
            "AVOID_LONG_TERM": 0
        },
        "errors": []
    }
    
    try:
        # Get all stocks for the date
        stocks = await db.stock_prices.find(
            {"date": trade_date},
            {"_id": 0}
        ).to_list(1000)
        
        # Get short-term signals for reference
        short_term_signals = {}
        cursor = db.trading_signals.find({"date": trade_date}, {"_id": 0})
        async for sig in cursor:
            short_term_signals[sig["symbol"]] = sig
        
        # Clear existing long-term signals for this date
        await db.long_term_signals.delete_many({"date": trade_date})
        
        long_term_signals = []
        
        for stock in stocks:
            # Only analyze stocks with "Analyzed" type
            if stock.get("stock_type") != "Analyzed":
                continue
            
            stats["total_analyzed"] += 1
            
            symbol = stock.get("symbol")
            short_term = short_term_signals.get(symbol)
            
            # Calculate component scores
            business_strength = calculate_long_term_business_strength(stock, short_term)
            financial_stability = calculate_financial_stability(stock, short_term)
            consistency_quality = calculate_consistency_quality(stock, short_term)
            trend_stability = calculate_trend_stability(stock, short_term)
            data_quality = calculate_data_quality_lt(stock, short_term)
            
            # Calculate overall score
            lt_score = calculate_long_term_score(
                business_strength,
                financial_stability,
                consistency_quality,
                trend_stability,
                data_quality
            )
            
            # Determine signal
            lt_signal = determine_long_term_signal(lt_score)
            
            # Generate reason
            scores = {
                "business_strength": business_strength,
                "financial_stability": financial_stability,
                "consistency_quality": consistency_quality,
                "trend_stability": trend_stability,
                "data_quality": data_quality
            }
            reason = generate_long_term_reason(symbol, lt_signal, scores)
            
            # Create long-term signal document
            lt_doc = {
                "symbol": symbol,
                "name": stock.get("name", ""),
                "date": trade_date,
                "signal_type": lt_signal,
                "long_term_score": lt_score,
                "business_strength_score": round(business_strength, 2),
                "financial_stability_score": round(financial_stability, 2),
                "consistency_quality_score": round(consistency_quality, 2),
                "trend_stability_score": round(trend_stability, 2),
                "data_quality_score": round(data_quality, 2),
                "current_price": stock.get("current_price"),
                "reason": reason,
                "short_term_signal": short_term.get("signalType") if short_term else None,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            long_term_signals.append(lt_doc)
            stats["by_signal"][lt_signal] += 1
        
        # Insert all long-term signals
        if long_term_signals:
            await db.long_term_signals.insert_many(long_term_signals)
            stats["signals_generated"] = len(long_term_signals)
        
        logger.info(f"Generated {len(long_term_signals)} long-term signals for {trade_date}")
        
    except Exception as e:
        logger.error(f"Long-term signal generation error: {e}")
        stats["errors"].append(str(e))
    
    return stats


async def get_long_term_signal(db, symbol: str, trade_date: str = None) -> Optional[Dict[str, Any]]:
    """Get long-term signal for a specific stock."""
    query = {"symbol": symbol}
    if trade_date:
        query["date"] = trade_date
    
    signal = await db.long_term_signals.find_one(
        query,
        {"_id": 0},
        sort=[("date", -1)]
    )
    
    return signal


async def get_long_term_signals_by_type(
    db,
    signal_type: str,
    trade_date: str = None,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """Get long-term signals filtered by type."""
    query = {"signal_type": signal_type}
    if trade_date:
        query["date"] = trade_date
    
    signals = await db.long_term_signals.find(
        query,
        {"_id": 0}
    ).sort("long_term_score", -1).limit(limit).to_list(limit)
    
    return signals
