#!/usr/bin/env python3
"""
NGX Signal Generation Engine

Generates trading signals based on VERIFIED official NGX price data.
Rule-based, transparent, reproducible logic.

NO random picks. NO fake signals. ONLY data-driven analysis.
"""

from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
from datetime import datetime
import uuid

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
trading_signals_collection = db.trading_signals


class SignalEngine:
    """
    Rule-based signal generation engine.
    Uses transparent, reproducible scoring logic.
    """
    
    def __init__(self):
        self.blue_chip_symbols = [
            'DANGCEM', 'MTNN', 'AIRTELAFRI', 'SEPLAT', 'NESTLE',
            'ZENITHBANK', 'UBA', 'GTCO', 'ACCESSCORP', 'FBNH'
        ]
        
        self.tier_1_banks = ['ZENITHBANK', 'UBA', 'GTCO', 'ACCESSCORP', 'FBNH']
        
    def calculate_business_score(self, stock):
        """
        Business Score: Company quality and market position.
        
        Factors:
        - Blue chip status (higher weight)
        - Price tier (higher price usually means larger cap)
        - Market category
        """
        score = 5.0  # Base score
        
        symbol = stock['symbol']
        price = stock['current_price']
        
        # Blue chip bonus
        if symbol in self.blue_chip_symbols:
            score += 2.5
        
        # Tier 1 bank bonus
        if symbol in self.tier_1_banks:
            score += 1.5
        
        # Price tier (proxy for market cap)
        if price > 500:  # Large cap (cement, telecom)
            score += 1.5
        elif price > 100:  # Mid-large cap
            score += 1.0
        elif price > 50:  # Mid cap
            score += 0.5
        elif price < 5:  # Small cap (higher risk)
            score -= 1.0
        
        # Analyzed stock (full data available)
        if stock.get('stock_type') == 'Analyzed':
            score += 0.5
        
        return min(max(score, 1.0), 10.0)  # Clamp between 1-10
    
    def calculate_setup_score(self, stock, prev_stock=None):
        """
        Setup Score: Technical position and momentum.
        
        Factors:
        - Price change trend
        - Price stability
        - Distance from psychological levels
        """
        score = 5.0  # Base score
        
        current = stock['current_price']
        previous = stock['previous_close']
        
        # Calculate change
        if previous > 0:
            change_pct = ((current - previous) / previous) * 100
        else:
            change_pct = 0
        
        # Momentum factor
        if change_pct > 5:  # Strong upward momentum
            score += 2.0
        elif change_pct > 2:  # Moderate upward
            score += 1.0
        elif change_pct > 0:  # Slight upward
            score += 0.5
        elif change_pct < -5:  # Strong downward (potential bottom)
            score -= 1.0
        elif change_pct < -2:  # Moderate downward
            score -= 0.5
        
        # Price stability (prefer not at extremes)
        if current == previous:  # No movement (could be good or bad)
            score += 0.3  # Slight bonus for stability
        
        # Psychological level proximity (round numbers)
        if current >= 100:
            # Check if near round 50 or 100
            remainder_100 = current % 100
            remainder_50 = current % 50
            if remainder_100 < 5 or remainder_100 > 95:
                score += 0.5  # Near psychological level
            elif remainder_50 < 5 or remainder_50 > 45:
                score += 0.3
        
        return min(max(score, 1.0), 10.0)
    
    def calculate_data_score(self, stock):
        """
        Data Score: Data quality and completeness.
        
        Factors:
        - All required fields present
        - Data source reliability
        - Price validity
        """
        score = 8.0  # Start high (we have official data)
        
        # Check required fields
        if not stock.get('current_price') or stock['current_price'] <= 0:
            score -= 3.0
        
        if not stock.get('previous_close') or stock['previous_close'] <= 0:
            score -= 1.0
        
        if not stock.get('official_close'):
            score -= 1.0
        
        # Source quality
        if stock.get('source_type') == 'official_pdf':
            score += 2.0  # Official NGX source
        
        # Stock type
        if stock.get('stock_type') == 'Analyzed':
            score += 0.5
        else:
            score -= 2.0  # Price-only has less data
        
        return min(max(score, 1.0), 10.0)
    
    def calculate_opportunity_score(self, business, setup, data):
        """
        Opportunity Score: Overall opportunity assessment.
        
        Weighted combination of other scores with additional factors.
        """
        # Weighted average
        weighted = (business * 0.4) + (setup * 0.4) + (data * 0.2)
        
        # Bonus for strong combination
        if business >= 8 and setup >= 7:
            weighted += 1.0
        
        # Penalty for weak data
        if data < 6:
            weighted -= 1.0
        
        return min(max(weighted, 1.0), 10.0)
    
    def determine_signal_type(self, opportunity_score, business_score, setup_score):
        """
        Determine signal type based on scores.
        
        Rules:
        - Buy Candidate: Opportunity >= 7.5 AND Business >= 7
        - Watchlist: Opportunity >= 6.0
        - Speculative: Opportunity >= 4.5 AND Setup >= 5
        - Avoid: Everything else
        
        Note: "Best Trade of the Day" is selected separately as the 
        highest-scoring Buy Candidate (not a threshold-based category).
        """
        if opportunity_score >= 7.5 and business_score >= 7:
            return "Buy Candidate"
        elif opportunity_score >= 6.0:
            return "Watchlist"
        elif opportunity_score >= 4.5 and setup_score >= 5:
            return "Speculative"
        else:
            return "Avoid"
    
    def calculate_confidence_level(self, opportunity_score, data_score):
        """
        Calculate confidence level for the signal.
        """
        base_confidence = (opportunity_score + data_score) / 2
        
        if base_confidence >= 8.5:
            return "Very High"
        elif base_confidence >= 7.5:
            return "High"
        elif base_confidence >= 6.5:
            return "Medium"
        elif base_confidence >= 5.5:
            return "Low"
        else:
            return "Very Low"
    
    def generate_explanation(self, stock, signal_type, business, setup, opportunity):
        """
        Generate human-readable explanation for the signal.
        """
        symbol = stock['symbol']
        price = stock['current_price']
        
        reasons = []
        
        # Business reasons
        if symbol in self.blue_chip_symbols:
            reasons.append("blue-chip stock with strong market position")
        
        if business >= 8:
            reasons.append("excellent business fundamentals")
        elif business >= 7:
            reasons.append("solid business quality")
        
        # Setup reasons
        if stock['current_price'] != stock['previous_close']:
            change_pct = ((stock['current_price'] - stock['previous_close']) / stock['previous_close']) * 100
            if change_pct > 2:
                reasons.append(f"strong upward momentum (+{change_pct:.1f}%)")
            elif change_pct > 0:
                reasons.append(f"positive price action (+{change_pct:.1f}%)")
        
        # Price level
        if price > 500:
            reasons.append("large-cap stability")
        elif price > 100:
            reasons.append("mid-to-large cap position")
        
        # Opportunity
        if opportunity >= 8.5:
            reasons.append("exceptional risk/reward ratio")
        elif opportunity >= 7.5:
            reasons.append("attractive opportunity")
        
        if not reasons:
            reasons.append("standard market opportunity")
        
        return f"{symbol}: " + ", ".join(reasons) + "."
    
    async def generate_signals(self, trade_date):
        """
        Generate signals for all analyzed stocks on a given date.
        
        Returns: List of generated signals
        """
        date_str = trade_date.strftime("%Y-%m-%d")
        
        logger.info(f"Generating signals for {date_str}")
        
        # Get all ANALYZED stocks for this date
        stocks = await stock_prices_collection.find({
            "date": date_str,
            "stock_type": "Analyzed"
        }, {"_id": 0}).to_list(500)
        
        if not stocks:
            logger.error(f"No analyzed stocks found for {date_str}")
            return []
        
        logger.info(f"Found {len(stocks)} analyzed stocks")
        
        signals = []
        
        for stock in stocks:
            try:
                # Calculate scores
                business_score = self.calculate_business_score(stock)
                setup_score = self.calculate_setup_score(stock)
                data_score = self.calculate_data_score(stock)
                opportunity_score = self.calculate_opportunity_score(
                    business_score, setup_score, data_score
                )
                
                # Determine signal type
                signal_type = self.determine_signal_type(
                    opportunity_score, business_score, setup_score
                )
                
                # Calculate confidence
                confidence = self.calculate_confidence_level(
                    opportunity_score, data_score
                )
                
                # Generate explanation
                explanation = self.generate_explanation(
                    stock, signal_type, business_score, setup_score, opportunity_score
                )
                
                # Create signal
                signal = {
                    "id": str(uuid.uuid4()),
                    "date": date_str,
                    "symbol": stock['symbol'],
                    "name": stock['name'],
                    "signalType": signal_type,
                    "entryPrice": stock['current_price'],
                    "businessScore": round(business_score, 2),
                    "setupScore": round(setup_score, 2),
                    "dataScore": round(data_score, 2),
                    "opportunityScore": round(opportunity_score, 2),
                    "overallScore": round(opportunity_score, 2),
                    "confidenceLevel": confidence,
                    "reason": explanation,
                    "targetPrice": round(stock['current_price'] * 1.15, 2),  # 15% target
                    "stopLoss": round(stock['current_price'] * 0.92, 2),  # 8% stop
                    "generated_at": datetime.now().isoformat()
                }
                
                signals.append(signal)
                
            except Exception as e:
                logger.error(f"Error generating signal for {stock['symbol']}: {e}")
                continue
        
        # Sort by opportunity score (highest first)
        signals.sort(key=lambda x: x['opportunityScore'], reverse=True)
        
        logger.info(f"✅ Generated {len(signals)} signals")
        
        # Log signal type distribution
        signal_counts = {}
        for sig in signals:
            sig_type = sig['signalType']
            signal_counts[sig_type] = signal_counts.get(sig_type, 0) + 1
        
        logger.info("\nSignal distribution:")
        for sig_type, count in signal_counts.items():
            logger.info(f"  {sig_type}: {count}")
        
        return signals
    
    async def store_signals(self, signals):
        """
        Store generated signals in MongoDB.
        Ensures exactly ONE signal per stock (no duplicates).
        """
        if not signals:
            return 0
        
        # Clear existing signals for this date first
        if signals:
            date = signals[0]['date']
            await trading_signals_collection.delete_many({'date': date})
            logger.info(f"Cleared existing signals for {date}")
        
        stored_count = 0
        
        for signal in signals:
            # Use symbol+date as unique key
            await trading_signals_collection.update_one(
                {'symbol': signal['symbol'], 'date': signal['date']},
                {'$set': signal},
                upsert=True
            )
            stored_count += 1
        
        logger.info(f"✅ Stored {stored_count} signals in database (one per stock)")
        return stored_count


async def main():
    """Main signal generation workflow."""
    engine = SignalEngine()
    
    logger.info("="*60)
    logger.info("NGX SIGNAL GENERATION")
    logger.info("="*60)
    
    # Find latest available date in database
    latest_doc = await stock_prices_collection.find_one(
        {},
        {"_id": 0, "date": 1},
        sort=[("date", -1)]
    )
    
    if not latest_doc:
        logger.error("No price data available in database")
        return
    
    latest_date_str = latest_doc['date']
    latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
    
    logger.info(f"Using latest available date: {latest_date_str}")
    
    # Generate signals
    signals = await engine.generate_signals(latest_date)
    
    if signals:
        # Store in database
        await engine.store_signals(signals)
        
        # Show best signals
        logger.info("\n" + "="*60)
        logger.info("TOP SIGNALS")
        logger.info("="*60)
        
        for i, signal in enumerate(signals[:10], 1):
            logger.info(f"\n{i}. {signal['symbol']} - {signal['signalType']}")
            logger.info(f"   Score: {signal['opportunityScore']:.2f} ({signal['confidenceLevel']})")
            logger.info(f"   Price: ₦{signal['entryPrice']:,.2f}")
            logger.info(f"   {signal['reason']}")
    
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
