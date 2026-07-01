import hashlib
import os
import secrets
import time
from typing import Dict, List, Optional, Tuple, Callable
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
import jwt

SECRET_KEY = os.environ.get("BRIDGE_SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError(
        "BRIDGE_SECRET_KEY environment variable must be set"
    )

ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/admin/login")

# --- Password hashing (PBKDF2 via stdlib) ---
_PBKDF2_ITERATIONS = 600000
_SALT_BYTES = 32

ADMIN_PASSWORD = os.environ.get("BRIDGE_ADMIN_PASSWORD", "admin123")
VIEWER_PASSWORD = os.environ.get("BRIDGE_VIEWER_PASSWORD", "viewer123")

_ADMIN_PASSWORD_HASH = None
_VIEWER_PASSWORD_HASH = None


def _hash_password(password: str, salt: Optional[bytes] = None) -> Tuple[bytes, bytes]:
    if salt is None:
        salt = os.urandom(_SALT_BYTES)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS)
    return salt, dk


def _verify_password(password: str, salt: bytes, stored_hash: bytes) -> bool:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS)
    return dk == stored_hash


def _encode_password_hash(salt: bytes, dk: bytes) -> str:
    return f"{salt.hex()}${dk.hex()}"


def _decode_password_hash(encoded: str) -> Tuple[bytes, bytes]:
    salt_hex, dk_hex = encoded.split("$")
    return bytes.fromhex(salt_hex), bytes.fromhex(dk_hex)


def _init_password_hashes():
    global _ADMIN_PASSWORD_HASH, _VIEWER_PASSWORD_HASH
    if _ADMIN_PASSWORD_HASH is None:
        salt, dk = _hash_password(ADMIN_PASSWORD)
        _ADMIN_PASSWORD_HASH = _encode_password_hash(salt, dk)
    if _VIEWER_PASSWORD_HASH is None:
        salt, dk = _hash_password(VIEWER_PASSWORD)
        _VIEWER_PASSWORD_HASH = _encode_password_hash(salt, dk)


_init_password_hashes()


# --- Password policy ---
PASSWORD_MIN_LENGTH = int(os.environ.get("BRIDGE_PASSWORD_MIN_LENGTH", "8"))
PASSWORD_REQUIRE_UPPER = os.environ.get("BRIDGE_PASSWORD_REQUIRE_UPPER", "1") == "1"
PASSWORD_REQUIRE_DIGIT = os.environ.get("BRIDGE_PASSWORD_REQUIRE_DIGIT", "1") == "1"
PASSWORD_REQUIRE_SPECIAL = os.environ.get("BRIDGE_PASSWORD_REQUIRE_SPECIAL", "0") == "1"


def validate_password_policy(password: str) -> Optional[str]:
    if len(password) < PASSWORD_MIN_LENGTH:
        return f"Password must be at least {PASSWORD_MIN_LENGTH} characters long"
    if PASSWORD_REQUIRE_UPPER and not any(c.isupper() for c in password):
        return "Password must contain at least one uppercase letter"
    if PASSWORD_REQUIRE_DIGIT and not any(c.isdigit() for c in password):
        return "Password must contain at least one digit"
    if PASSWORD_REQUIRE_SPECIAL and not any(c in "!@#$%^&*()_+-=[]{}|;':\",./<>?`~" for c in password):
        return "Password must contain at least one special character"
    return None


# --- Account lockout ---
_MAX_LOGIN_ATTEMPTS = int(os.environ.get("BRIDGE_MAX_LOGIN_ATTEMPTS", "5"))
_LOCKOUT_DURATION = int(os.environ.get("BRIDGE_LOCKOUT_DURATION", "900"))  # 15 minutes

_login_attempts: Dict[str, List[float]] = {}


def _check_lockout(username: str) -> None:
    now = time.time()
    attempts = _login_attempts.get(username, [])
    valid_attempts = [t for t in attempts if now - t < _LOCKOUT_DURATION]
    _login_attempts[username] = valid_attempts
    if len(valid_attempts) >= _MAX_LOGIN_ATTEMPTS:
        remaining = int(_LOCKOUT_DURATION - (now - valid_attempts[0]))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Account locked due to too many failed login attempts. Retry after {remaining} seconds.",
        )


def _record_failed_attempt(username: str) -> None:
    now = time.time()
    attempts = _login_attempts.get(username, [])
    attempts.append(now)
    _login_attempts[username] = attempts


def _clear_attempts(username: str) -> None:
    _login_attempts.pop(username, None)


def verify_credentials(username: str, password: str) -> Optional[User]:
    _check_lockout(username)

    if username == "admin":
        salt, stored_dk = _decode_password_hash(_ADMIN_PASSWORD_HASH)
        if _verify_password(password, salt, stored_dk):
            _clear_attempts(username)
            return User(username="admin", roles=["admin"])
    elif username == "viewer":
        salt, stored_dk = _decode_password_hash(_VIEWER_PASSWORD_HASH)
        if _verify_password(password, salt, stored_dk):
            _clear_attempts(username)
            return User(username="viewer", roles=["viewer"])

    _record_failed_attempt(username)
    return None


# --- User model ---
class User(BaseModel):
    username: str
    roles: List[str] = []


def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        roles: List[str] = payload.get("roles", [])
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return User(username=username, roles=roles)
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_role(allowed_roles: List[str]):
    def role_checker(user: User = Depends(get_current_user)):
        if not any(role in allowed_roles for role in user.roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not enough permissions"
            )
        return user
    return role_checker
