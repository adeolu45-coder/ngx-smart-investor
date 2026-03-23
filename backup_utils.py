"""
NGX Smart Investor - Backup & Recovery Utilities
Handles database backup, restoration, and integrity verification.
"""

import os
import json
import gzip
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import asyncio

logger = logging.getLogger(__name__)

# Backup directory
BACKUP_DIR = Path("/app/backups")
BACKUP_DIR.mkdir(exist_ok=True)

# Collections to backup
BACKUP_COLLECTIONS = [
    "stock_prices",
    "trading_signals",
    "long_term_signals",
    "alerts",
    "user_watchlist",
    "audit_logs"
]


async def backup_collection(db, collection_name: str, backup_path: Path) -> Dict[str, Any]:
    """Backup a single collection to a gzipped JSON file."""
    result = {
        "collection": collection_name,
        "success": False,
        "documents": 0,
        "file_path": None,
        "error": None
    }
    
    try:
        collection = db[collection_name]
        documents = await collection.find({}, {"_id": 0}).to_list(None)
        
        file_path = backup_path / f"{collection_name}.json.gz"
        
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            json.dump(documents, f, default=str)
        
        result["success"] = True
        result["documents"] = len(documents)
        result["file_path"] = str(file_path)
        
        logger.info(f"Backed up {len(documents)} documents from {collection_name}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Backup failed for {collection_name}: {e}")
    
    return result


async def create_full_backup(db) -> Dict[str, Any]:
    """Create a full backup of all important collections."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"backup_{timestamp}"
    backup_path.mkdir(exist_ok=True)
    
    backup_result = {
        "timestamp": timestamp,
        "backup_path": str(backup_path),
        "collections": {},
        "total_documents": 0,
        "success": True,
        "errors": []
    }
    
    for collection_name in BACKUP_COLLECTIONS:
        result = await backup_collection(db, collection_name, backup_path)
        backup_result["collections"][collection_name] = result
        backup_result["total_documents"] += result.get("documents", 0)
        
        if not result["success"]:
            backup_result["errors"].append(f"{collection_name}: {result['error']}")
    
    if backup_result["errors"]:
        backup_result["success"] = False
    
    # Create metadata file
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "collections": list(backup_result["collections"].keys()),
        "total_documents": backup_result["total_documents"],
        "success": backup_result["success"]
    }
    
    with open(backup_path / "metadata.json", 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Log to database
    try:
        await db.backup_logs.insert_one({
            "timestamp": timestamp,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "backup_path": str(backup_path),
            "total_documents": backup_result["total_documents"],
            "success": backup_result["success"],
            "errors": backup_result["errors"]
        })
    except Exception as e:
        logger.error(f"Failed to log backup: {e}")
    
    logger.info(f"Full backup completed: {backup_result['total_documents']} documents")
    
    return backup_result


async def restore_collection(db, collection_name: str, backup_path: Path, clear_existing: bool = True) -> Dict[str, Any]:
    """Restore a collection from backup."""
    result = {
        "collection": collection_name,
        "success": False,
        "documents_restored": 0,
        "error": None
    }
    
    try:
        file_path = backup_path / f"{collection_name}.json.gz"
        
        if not file_path.exists():
            result["error"] = "Backup file not found"
            return result
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            documents = json.load(f)
        
        collection = db[collection_name]
        
        if clear_existing:
            await collection.delete_many({})
        
        if documents:
            await collection.insert_many(documents)
        
        result["success"] = True
        result["documents_restored"] = len(documents)
        
        logger.info(f"Restored {len(documents)} documents to {collection_name}")
        
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Restore failed for {collection_name}: {e}")
    
    return result


async def restore_from_backup(db, backup_timestamp: str, collections: List[str] = None) -> Dict[str, Any]:
    """Restore from a specific backup."""
    backup_path = BACKUP_DIR / f"backup_{backup_timestamp}"
    
    if not backup_path.exists():
        return {
            "success": False,
            "error": f"Backup not found: {backup_timestamp}"
        }
    
    collections_to_restore = collections or BACKUP_COLLECTIONS
    
    restore_result = {
        "backup_timestamp": backup_timestamp,
        "collections": {},
        "total_restored": 0,
        "success": True,
        "errors": []
    }
    
    for collection_name in collections_to_restore:
        result = await restore_collection(db, collection_name, backup_path)
        restore_result["collections"][collection_name] = result
        restore_result["total_restored"] += result.get("documents_restored", 0)
        
        if not result["success"]:
            restore_result["errors"].append(f"{collection_name}: {result['error']}")
    
    if restore_result["errors"]:
        restore_result["success"] = False
    
    logger.info(f"Restore completed: {restore_result['total_restored']} documents")
    
    return restore_result


async def list_backups() -> List[Dict[str, Any]]:
    """List all available backups."""
    backups = []
    
    for backup_dir in sorted(BACKUP_DIR.iterdir(), reverse=True):
        if backup_dir.is_dir() and backup_dir.name.startswith("backup_"):
            metadata_file = backup_dir / "metadata.json"
            
            backup_info = {
                "timestamp": backup_dir.name.replace("backup_", ""),
                "path": str(backup_dir)
            }
            
            if metadata_file.exists():
                with open(metadata_file) as f:
                    metadata = json.load(f)
                    backup_info.update(metadata)
            
            backups.append(backup_info)
    
    return backups


async def get_last_backup_info(db) -> Optional[Dict[str, Any]]:
    """Get information about the last successful backup."""
    try:
        backup = await db.backup_logs.find_one(
            {"success": True},
            {"_id": 0},
            sort=[("created_at", -1)]
        )
        return backup
    except Exception:
        return None


async def cleanup_old_backups(keep_count: int = 7):
    """Remove old backups, keeping only the most recent N."""
    backups = await list_backups()
    
    if len(backups) <= keep_count:
        return {"removed": 0}
    
    to_remove = backups[keep_count:]
    removed = 0
    
    for backup in to_remove:
        try:
            backup_path = Path(backup["path"])
            for file in backup_path.iterdir():
                file.unlink()
            backup_path.rmdir()
            removed += 1
            logger.info(f"Removed old backup: {backup['timestamp']}")
        except Exception as e:
            logger.error(f"Failed to remove backup {backup['timestamp']}: {e}")
    
    return {"removed": removed}


# ============================================
# DATABASE INDEXES
# ============================================

async def create_database_indexes(db):
    """Create necessary indexes for performance and uniqueness."""
    indexes_created = []
    
    try:
        # stock_prices: unique on symbol + date
        await db.stock_prices.create_index(
            [("symbol", 1), ("date", 1)],
            unique=True,
            name="stock_prices_symbol_date_unique"
        )
        indexes_created.append("stock_prices_symbol_date_unique")
        
        # trading_signals: unique on symbol + date
        await db.trading_signals.create_index(
            [("symbol", 1), ("date", 1)],
            unique=True,
            name="trading_signals_symbol_date_unique"
        )
        indexes_created.append("trading_signals_symbol_date_unique")
        
        # long_term_signals: unique on symbol + date
        await db.long_term_signals.create_index(
            [("symbol", 1), ("date", 1)],
            unique=True,
            name="long_term_signals_symbol_date_unique"
        )
        indexes_created.append("long_term_signals_symbol_date_unique")
        
        # alerts: index for queries
        await db.alerts.create_index(
            [("symbol", 1), ("type", 1), ("trade_date", 1)],
            name="alerts_symbol_type_date"
        )
        indexes_created.append("alerts_symbol_type_date")
        
        await db.alerts.create_index(
            [("is_dismissed", 1), ("created_at", -1)],
            name="alerts_dismissed_created"
        )
        indexes_created.append("alerts_dismissed_created")
        
        # user_watchlist: unique on user_id + symbol
        await db.user_watchlist.create_index(
            [("user_id", 1), ("symbol", 1)],
            unique=True,
            name="watchlist_user_symbol_unique"
        )
        indexes_created.append("watchlist_user_symbol_unique")
        
        # audit_logs: index for queries
        await db.audit_logs.create_index(
            [("timestamp", -1)],
            name="audit_logs_timestamp"
        )
        indexes_created.append("audit_logs_timestamp")
        
        await db.audit_logs.create_index(
            [("action", 1), ("timestamp", -1)],
            name="audit_logs_action_timestamp"
        )
        indexes_created.append("audit_logs_action_timestamp")
        
        logger.info(f"Created {len(indexes_created)} database indexes")
        
    except Exception as e:
        logger.error(f"Index creation error: {e}")
    
    return indexes_created


async def verify_database_integrity(db) -> Dict[str, Any]:
    """Verify database integrity and report issues."""
    issues = []
    stats = {}
    
    # Check collection counts
    for collection_name in BACKUP_COLLECTIONS:
        try:
            count = await db[collection_name].count_documents({})
            stats[collection_name] = count
        except Exception as e:
            issues.append(f"Failed to count {collection_name}: {e}")
    
    # Check for orphaned data
    try:
        # Get latest date from stock_prices
        latest_price = await db.stock_prices.find_one(
            {},
            {"_id": 0, "date": 1},
            sort=[("date", -1)]
        )
        
        if latest_price:
            stats["latest_price_date"] = latest_price["date"]
            
            # Check if signals exist for this date
            signal_count = await db.trading_signals.count_documents(
                {"date": latest_price["date"]}
            )
            stats["signals_for_latest_date"] = signal_count
            
            if signal_count == 0:
                issues.append("No signals found for latest price date")
    except Exception as e:
        issues.append(f"Integrity check error: {e}")
    
    return {
        "stats": stats,
        "issues": issues,
        "healthy": len(issues) == 0
    }
