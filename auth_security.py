"""
NGX Smart Investor - Authentication & Security Module
Handles JWT authentication, password hashing, rate limiting, and audit logging.
"""

import os
import jwt
import bcrypt
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from functools import wraps
from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uuid
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================

# Load from environment variables (NEVER hardcode!)
ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
ADMIN_PASSWORD_HASH = os.environ.get('ADMIN_PASSWORD_HASH')  # Must be bcrypt hash
JWT_SECRET = os.environ.get('JWT_SECRET')
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = int(os.environ.get('JWT_EXPIRATION_HOURS', '24'))

# Security settings
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 15

# In-memory store for rate limiting login attempts (per IP)
login_attempts: Dict[str, Dict] = {}

# Security bearer scheme
security = HTTPBearer(auto_error=False)


# ============================================
# PASSWORD HASHING
# ============================================

def hash_password(plain_password: str) -> str:
    """Hash a password using bcrypt."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(plain_password.encode('utf-8'), salt).decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    try:
        return bcrypt.checkpw(
            plain_password.encode('utf-8'),
            hashed_password.encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        return False


def generate_password_hash_for_env(password: str) -> str:
    """
    Utility function to generate a password hash for .env file.
    Run this once to generate the hash, then store in .env.
    """
    return hash_password(password)


# ============================================
# JWT TOKEN MANAGEMENT
# ============================================

def create_access_token(username: str, role: str = "admin") -> Dict[str, Any]:
    """Create a JWT access token."""
    if not JWT_SECRET:
        raise ValueError("JWT_SECRET not configured in environment")
    
    now = datetime.now(timezone.utc)
    expiration = now + timedelta(hours=JWT_EXPIRATION_HOURS)
    
    payload = {
        "sub": username,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int(expiration.timestamp()),
        "jti": str(uuid.uuid4())  # Unique token ID
    }
    
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": JWT_EXPIRATION_HOURS * 3600,
        "expires_at": expiration.isoformat()
    }


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify and decode a JWT token."""
    if not JWT_SECRET:
        logger.error("JWT_SECRET not configured")
        return None
    
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        return None


# ============================================
# LOGIN RATE LIMITING
# ============================================

def check_login_rate_limit(ip_address: str) -> bool:
    """Check if IP is rate limited for login attempts."""
    if ip_address not in login_attempts:
        return True
    
    attempts = login_attempts[ip_address]
    
    # Check if lockout period has passed
    if attempts.get('locked_until'):
        if datetime.now(timezone.utc) < attempts['locked_until']:
            return False
        else:
            # Lockout expired, reset
            login_attempts[ip_address] = {'count': 0, 'locked_until': None}
            return True
    
    return attempts.get('count', 0) < MAX_LOGIN_ATTEMPTS


def record_login_attempt(ip_address: str, success: bool):
    """Record a login attempt."""
    if ip_address not in login_attempts:
        login_attempts[ip_address] = {'count': 0, 'locked_until': None}
    
    if success:
        # Reset on successful login
        login_attempts[ip_address] = {'count': 0, 'locked_until': None}
    else:
        login_attempts[ip_address]['count'] += 1
        
        # Check if should lock
        if login_attempts[ip_address]['count'] >= MAX_LOGIN_ATTEMPTS:
            login_attempts[ip_address]['locked_until'] = (
                datetime.now(timezone.utc) + timedelta(minutes=LOCKOUT_DURATION_MINUTES)
            )
            logger.warning(f"IP {ip_address} locked out due to too many failed attempts")


def get_remaining_lockout_time(ip_address: str) -> Optional[int]:
    """Get remaining lockout time in seconds."""
    if ip_address not in login_attempts:
        return None
    
    attempts = login_attempts[ip_address]
    if not attempts.get('locked_until'):
        return None
    
    remaining = (attempts['locked_until'] - datetime.now(timezone.utc)).total_seconds()
    return max(0, int(remaining))


# ============================================
# AUTHENTICATION DEPENDENCIES
# ============================================

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Optional[Dict[str, Any]]:
    """Get current authenticated user from token."""
    if not credentials:
        return None
    
    payload = verify_token(credentials.credentials)
    if not payload:
        return None
    
    return {
        "username": payload.get("sub"),
        "role": payload.get("role"),
        "token_id": payload.get("jti")
    }


async def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """Require admin authentication for protected endpoints."""
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    payload = verify_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    if payload.get("role") != "admin":
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )
    
    return {
        "username": payload.get("sub"),
        "role": payload.get("role"),
        "token_id": payload.get("jti")
    }


# ============================================
# AUDIT LOGGING
# ============================================

class AuditLogger:
    """Audit logger for sensitive actions."""
    
    def __init__(self, db):
        self.db = db
        self.collection = db.audit_logs
    
    async def log(
        self,
        action: str,
        actor: str = "system",
        endpoint: str = None,
        status: str = "success",
        details: Dict[str, Any] = None,
        ip_address: str = None,
        affected_resource: str = None
    ):
        """Log an audit event."""
        log_entry = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "actor": actor,
            "endpoint": endpoint,
            "status": status,
            "details": details or {},
            "ip_address": ip_address,
            "affected_resource": affected_resource
        }
        
        try:
            await self.collection.insert_one(log_entry)
            logger.info(f"AUDIT: {action} by {actor} - {status}")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")
    
    async def get_recent_logs(self, limit: int = 100, action_filter: str = None):
        """Get recent audit logs."""
        query = {}
        if action_filter:
            query["action"] = action_filter
        
        logs = await self.collection.find(
            query,
            {"_id": 0}
        ).sort("timestamp", -1).limit(limit).to_list(limit)
        
        return logs
    
    async def get_failed_logins(self, hours: int = 24):
        """Get failed login attempts in the last N hours."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        
        count = await self.collection.count_documents({
            "action": "login_attempt",
            "status": "failed",
            "timestamp": {"$gte": cutoff}
        })
        
        return count


# ============================================
# INPUT VALIDATION
# ============================================

def validate_symbol(symbol: str) -> str:
    """Validate and sanitize stock symbol."""
    if not symbol:
        raise ValueError("Symbol is required")
    
    # Only allow alphanumeric and underscore
    sanitized = ''.join(c for c in symbol.upper() if c.isalnum() or c == '_')
    
    if len(sanitized) < 1 or len(sanitized) > 20:
        raise ValueError("Invalid symbol length")
    
    return sanitized


def validate_date(date_str: str) -> str:
    """Validate date format (YYYY-MM-DD)."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD")


def validate_positive_number(value: float, name: str) -> float:
    """Validate positive number."""
    if value is None or value < 0:
        raise ValueError(f"{name} must be a positive number")
    return value


# ============================================
# UTILITY FUNCTIONS
# ============================================

def get_client_ip(request: Request) -> str:
    """Get client IP address from request."""
    # Check for forwarded header (behind proxy)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    
    return request.client.host if request.client else "unknown"


def mask_sensitive_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Mask sensitive fields in data for logging."""
    sensitive_fields = ['password', 'token', 'secret', 'api_key', 'authorization']
    masked = {}
    
    for key, value in data.items():
        if any(s in key.lower() for s in sensitive_fields):
            masked[key] = "***MASKED***"
        elif isinstance(value, dict):
            masked[key] = mask_sensitive_data(value)
        else:
            masked[key] = value
    
    return masked
