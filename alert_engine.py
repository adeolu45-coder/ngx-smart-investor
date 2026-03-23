"""
NGX Smart Investor - Alert Engine v2
Handles alert generation, price triggers, and notification tracking.
"""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import uuid
import logging

logger = logging.getLogger(__name__)

# ============================================
# ALERT TYPES - EXPANDED
# ============================================

SIGNAL_ALERT_TYPES = {
    "NEW_BUY_CANDIDATE": "New Buy Candidate signal generated",
    "NEW_WATCHLIST": "New Watchlist signal generated",
    "NEW_SPECULATIVE": "New Speculative signal generated",
    "SIGNAL_UPGRADE": "Signal upgraded",
    "SIGNAL_DOWNGRADE": "Signal downgraded",
    "WATCHLIST_TO_BUY": "Watchlist upgraded to Buy Candidate"
}

PRICE_ALERT_TYPES = {
    "TARGET_HIT": "Price reached target",
    "STOP_LOSS_HIT": "Price hit stop loss",
    "SIGNIFICANT_PRICE_MOVE": "Significant price movement detected"
}

ALL_ALERT_TYPES = {**SIGNAL_ALERT_TYPES, **PRICE_ALERT_TYPES}

ALERT_PRIORITY = {
    "STOP_LOSS_HIT": "critical",
    "TARGET_HIT": "high",
    "SIGNAL_UPGRADE": "high",
    "NEW_BUY_CANDIDATE": "high",
    "WATCHLIST_TO_BUY": "high",
    "SIGNIFICANT_PRICE_MOVE": "medium",
    "SIGNAL_DOWNGRADE": "medium",
    "NEW_WATCHLIST": "medium",
    "NEW_SPECULATIVE": "low"
}

ALERT_CATEGORY = {
    "NEW_BUY_CANDIDATE": "signal",
    "NEW_WATCHLIST": "signal",
    "NEW_SPECULATIVE": "signal",
    "SIGNAL_UPGRADE": "signal",
    "SIGNAL_DOWNGRADE": "signal",
    "WATCHLIST_TO_BUY": "signal",
    "TARGET_HIT": "price",
    "STOP_LOSS_HIT": "price",
    "SIGNIFICANT_PRICE_MOVE": "price"
}

# Significant move threshold (percentage)
SIGNIFICANT_MOVE_THRESHOLD = 5.0  # 5% daily move


def generate_alert_id() -> str:
    return str(uuid.uuid4())


def create_alert(
    alert_type: str,
    symbol: str,
    name: str,
    message: str,
    trade_date: str,
    details: Dict[str, Any],
    priority: str = None
) -> Dict[str, Any]:
    """Create a new alert document with all required fields."""
    now = datetime.now(timezone.utc).isoformat()
    
    return {
        "id": generate_alert_id(),
        "type": alert_type,
        "category": ALERT_CATEGORY.get(alert_type, "signal"),
        "symbol": symbol,
        "name": name,
        "message": message,
        "trade_date": trade_date,
        "details": details,
        "priority": priority or ALERT_PRIORITY.get(alert_type, "medium"),
        "is_read": False,
        "is_dismissed": False,
        "status": "active",
        "created_at": now,
        "triggered_at": now
    }


async def check_price_crossing_alerts(
    db,
    current_prices: List[Dict[str, Any]],
    signals: Dict[str, Dict[str, Any]],
    trade_date: str
) -> List[Dict[str, Any]]:
    """
    Check if any stock's current price has crossed target or stop loss.
    This compares current_price against the signal's target_price and stop_loss.
    """
    alerts = []
    
    for price_doc in current_prices:
        symbol = price_doc.get("symbol")
        current_price = price_doc.get("current_price")
        previous_close = price_doc.get("previous_close")
        name = price_doc.get("name", "")
        
        if not symbol or not current_price:
            continue
        
        signal = signals.get(symbol)
        if not signal:
            continue
        
        target_price = signal.get("targetPrice")
        stop_loss = signal.get("stopLoss")
        entry_price = signal.get("entryPrice")
        signal_type = signal.get("signalType")
        confidence = signal.get("confidenceLevel")
        
        # Check TARGET_HIT: current price >= target price
        if target_price and current_price >= target_price:
            gain_pct = ((current_price - entry_price) / entry_price * 100) if entry_price else 0
            alerts.append(create_alert(
                alert_type="TARGET_HIT",
                symbol=symbol,
                name=name,
                message=f"{symbol} reached target price of ₦{target_price:,.2f}! Current: ₦{current_price:,.2f}",
                trade_date=trade_date,
                details={
                    "current_price": current_price,
                    "target_price": target_price,
                    "stop_loss": stop_loss,
                    "entry_price": entry_price,
                    "gain_percent": round(gain_pct, 2),
                    "signal_type": signal_type,
                    "confidence_level": confidence
                },
                priority="high"
            ))
        
        # Check STOP_LOSS_HIT: current price <= stop loss
        if stop_loss and current_price <= stop_loss:
            loss_pct = ((current_price - entry_price) / entry_price * 100) if entry_price else 0
            alerts.append(create_alert(
                alert_type="STOP_LOSS_HIT",
                symbol=symbol,
                name=name,
                message=f"{symbol} hit stop loss at ₦{stop_loss:,.2f}! Current: ₦{current_price:,.2f}",
                trade_date=trade_date,
                details={
                    "current_price": current_price,
                    "target_price": target_price,
                    "stop_loss": stop_loss,
                    "entry_price": entry_price,
                    "loss_percent": round(loss_pct, 2),
                    "signal_type": signal_type,
                    "confidence_level": confidence
                },
                priority="critical"
            ))
        
        # Check SIGNIFICANT_PRICE_MOVE: daily change > threshold
        if previous_close and previous_close > 0:
            change_pct = ((current_price - previous_close) / previous_close) * 100
            if abs(change_pct) >= SIGNIFICANT_MOVE_THRESHOLD:
                direction = "up" if change_pct > 0 else "down"
                alerts.append(create_alert(
                    alert_type="SIGNIFICANT_PRICE_MOVE",
                    symbol=symbol,
                    name=name,
                    message=f"{symbol} moved {abs(change_pct):.1f}% {direction} today! ₦{previous_close:,.2f} → ₦{current_price:,.2f}",
                    trade_date=trade_date,
                    details={
                        "current_price": current_price,
                        "previous_close": previous_close,
                        "change_percent": round(change_pct, 2),
                        "direction": direction,
                        "target_price": target_price,
                        "stop_loss": stop_loss,
                        "signal_type": signal_type,
                        "confidence_level": confidence
                    },
                    priority="medium"
                ))
    
    return alerts


async def check_signal_change_alerts(
    old_signals: Dict[str, Dict[str, Any]],
    new_signals: Dict[str, Dict[str, Any]],
    trade_date: str
) -> List[Dict[str, Any]]:
    """Check for signal upgrades/downgrades and new signals."""
    alerts = []
    
    # Signal hierarchy (higher = better)
    hierarchy = {
        "Buy Candidate": 4,
        "Watchlist": 3,
        "Speculative": 2,
        "Avoid": 1
    }
    
    for symbol, new_signal in new_signals.items():
        new_type = new_signal.get("signalType")
        name = new_signal.get("name", "")
        entry_price = new_signal.get("entryPrice")
        target_price = new_signal.get("targetPrice")
        stop_loss = new_signal.get("stopLoss")
        opp_score = new_signal.get("opportunityScore")
        confidence = new_signal.get("confidenceLevel")
        reason = new_signal.get("reason", "")
        
        old_signal = old_signals.get(symbol)
        old_type = old_signal.get("signalType") if old_signal else None
        
        new_rank = hierarchy.get(new_type, 0)
        old_rank = hierarchy.get(old_type, 0) if old_type else 0
        
        details = {
            "current_price": entry_price,
            "entry_price": entry_price,
            "target_price": target_price,
            "stop_loss": stop_loss,
            "opportunity_score": opp_score,
            "signal_type": new_type,
            "confidence_level": confidence,
            "previous_signal": old_type,
            "reason": reason
        }
        
        # New signal (no previous signal)
        if not old_signal:
            if new_type == "Buy Candidate":
                alerts.append(create_alert(
                    alert_type="NEW_BUY_CANDIDATE",
                    symbol=symbol,
                    name=name,
                    message=f"New Buy Candidate: {symbol} (Score: {opp_score:.2f}, Confidence: {confidence})",
                    trade_date=trade_date,
                    details=details,
                    priority="high"
                ))
            elif new_type == "Watchlist":
                alerts.append(create_alert(
                    alert_type="NEW_WATCHLIST",
                    symbol=symbol,
                    name=name,
                    message=f"New Watchlist: {symbol} (Score: {opp_score:.2f})",
                    trade_date=trade_date,
                    details=details,
                    priority="medium"
                ))
            elif new_type == "Speculative":
                alerts.append(create_alert(
                    alert_type="NEW_SPECULATIVE",
                    symbol=symbol,
                    name=name,
                    message=f"New Speculative: {symbol} (Score: {opp_score:.2f})",
                    trade_date=trade_date,
                    details=details,
                    priority="low"
                ))
        
        # Signal changed
        elif old_type != new_type:
            if new_rank > old_rank:
                # Upgrade
                if old_type == "Watchlist" and new_type == "Buy Candidate":
                    alerts.append(create_alert(
                        alert_type="WATCHLIST_TO_BUY",
                        symbol=symbol,
                        name=name,
                        message=f"{symbol} upgraded from Watchlist to Buy Candidate! (Score: {opp_score:.2f})",
                        trade_date=trade_date,
                        details=details,
                        priority="high"
                    ))
                else:
                    alerts.append(create_alert(
                        alert_type="SIGNAL_UPGRADE",
                        symbol=symbol,
                        name=name,
                        message=f"{symbol} upgraded: {old_type} → {new_type}",
                        trade_date=trade_date,
                        details=details,
                        priority="high"
                    ))
            else:
                # Downgrade
                alerts.append(create_alert(
                    alert_type="SIGNAL_DOWNGRADE",
                    symbol=symbol,
                    name=name,
                    message=f"{symbol} downgraded: {old_type} → {new_type}",
                    trade_date=trade_date,
                    details=details,
                    priority="medium"
                ))
    
    return alerts


async def generate_all_alerts(
    db,
    trade_date: str,
    check_price_triggers: bool = True,
    check_signal_changes: bool = True
) -> Dict[str, Any]:
    """
    Main alert generation function.
    Called after daily price ingestion.
    Returns generation stats.
    """
    generation_stats = {
        "trade_date": trade_date,
        "generation_time": datetime.now(timezone.utc).isoformat(),
        "signal_alerts": 0,
        "price_alerts": 0,
        "duplicates_prevented": 0,
        "total_generated": 0,
        "errors": []
    }
    
    try:
        # Get current prices
        current_prices = await db.stock_prices.find(
            {"date": trade_date},
            {"_id": 0}
        ).to_list(1000)
        
        # Get current signals
        current_signals = {}
        cursor = db.trading_signals.find({"date": trade_date}, {"_id": 0})
        async for sig in cursor:
            current_signals[sig["symbol"]] = sig
        
        # Get previous date signals (for change detection)
        date_obj = datetime.strptime(trade_date, "%Y-%m-%d")
        prev_date = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        
        prev_signals = {}
        cursor = db.trading_signals.find({"date": prev_date}, {"_id": 0})
        async for sig in cursor:
            prev_signals[sig["symbol"]] = sig
        
        all_alerts = []
        
        # Check signal changes
        if check_signal_changes:
            signal_alerts = await check_signal_change_alerts(
                prev_signals, current_signals, trade_date
            )
            all_alerts.extend(signal_alerts)
            generation_stats["signal_alerts"] = len(signal_alerts)
        
        # Check price triggers
        if check_price_triggers:
            price_alerts = await check_price_crossing_alerts(
                db, current_prices, current_signals, trade_date
            )
            all_alerts.extend(price_alerts)
            generation_stats["price_alerts"] = len(price_alerts)
        
        # Store alerts (prevent duplicates)
        stored_count = 0
        for alert in all_alerts:
            # Check for duplicate
            existing = await db.alerts.find_one({
                "symbol": alert["symbol"],
                "type": alert["type"],
                "trade_date": trade_date
            })
            
            if existing:
                generation_stats["duplicates_prevented"] += 1
                continue
            
            # Store alert
            alert_copy = dict(alert)
            await db.alerts.insert_one(alert_copy)
            stored_count += 1
        
        generation_stats["total_generated"] = stored_count
        
        # Update generation status
        await db.alert_generation_status.update_one(
            {"_id": "latest"},
            {
                "$set": {
                    "last_generation_time": generation_stats["generation_time"],
                    "last_trade_date": trade_date,
                    "alerts_generated": stored_count,
                    "signal_alerts": generation_stats["signal_alerts"],
                    "price_alerts": generation_stats["price_alerts"],
                    "duplicates_prevented": generation_stats["duplicates_prevented"]
                }
            },
            upsert=True
        )
        
        logger.info(f"Alert generation complete: {stored_count} alerts for {trade_date}")
        
    except Exception as e:
        logger.error(f"Alert generation error: {e}")
        generation_stats["errors"].append(str(e))
    
    return generation_stats


async def check_watchlist_alerts(
    db,
    trade_date: str,
    user_id: str = "default"
) -> List[Dict[str, Any]]:
    """
    Check watchlist stocks for price triggers.
    Creates alerts specific to user's watchlist.
    """
    alerts = []
    
    # Get user's watchlist
    watchlist = await db.user_watchlist.find(
        {"user_id": user_id},
        {"_id": 0}
    ).to_list(100)
    
    if not watchlist:
        return alerts
    
    symbols = [w["symbol"] for w in watchlist]
    
    # Get current prices
    prices = {}
    cursor = db.stock_prices.find(
        {"date": trade_date, "symbol": {"$in": symbols}},
        {"_id": 0}
    )
    async for p in cursor:
        prices[p["symbol"]] = p
    
    # Get signals
    signals = {}
    cursor = db.trading_signals.find(
        {"date": trade_date, "symbol": {"$in": symbols}},
        {"_id": 0}
    )
    async for s in cursor:
        signals[s["symbol"]] = s
    
    for item in watchlist:
        symbol = item["symbol"]
        price_doc = prices.get(symbol)
        signal = signals.get(symbol)
        
        if not price_doc or not signal:
            continue
        
        current_price = price_doc.get("current_price")
        target = item.get("custom_target") or signal.get("targetPrice")
        stop = item.get("custom_stop") or signal.get("stopLoss")
        entry = item.get("entry_price")
        name = item.get("name", "")
        
        # Check target hit
        if target and current_price >= target and item.get("alert_on_target", True):
            gain_pct = ((current_price - entry) / entry * 100) if entry else 0
            alerts.append(create_alert(
                alert_type="TARGET_HIT",
                symbol=symbol,
                name=name,
                message=f"WATCHLIST: {symbol} reached your target of ₦{target:,.2f}!",
                trade_date=trade_date,
                details={
                    "current_price": current_price,
                    "target_price": target,
                    "entry_price": entry,
                    "gain_percent": round(gain_pct, 2),
                    "signal_type": signal.get("signalType"),
                    "is_watchlist": True
                },
                priority="high"
            ))
        
        # Check stop loss hit
        if stop and current_price <= stop and item.get("alert_on_stop", True):
            loss_pct = ((current_price - entry) / entry * 100) if entry else 0
            alerts.append(create_alert(
                alert_type="STOP_LOSS_HIT",
                symbol=symbol,
                name=name,
                message=f"WATCHLIST: {symbol} hit your stop loss at ₦{stop:,.2f}!",
                trade_date=trade_date,
                details={
                    "current_price": current_price,
                    "stop_loss": stop,
                    "entry_price": entry,
                    "loss_percent": round(loss_pct, 2),
                    "signal_type": signal.get("signalType"),
                    "is_watchlist": True
                },
                priority="critical"
            ))
    
    return alerts
