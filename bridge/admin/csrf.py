import hmac
import secrets
from typing import Optional
from fastapi import HTTPException, Request, status
from starlette.datastructures import Headers


CSRF_COOKIE_NAME = "bridge_csrf_token"
CSRF_HEADER_NAME = "X-CSRF-Token"


def generate_csrf_token() -> str:
    return secrets.token_hex(32)


def validate_csrf_token(cookie_token: Optional[str], header_token: Optional[str]) -> bool:
    if not cookie_token or not header_token:
        return False
    return hmac.compare_digest(cookie_token, header_token)


async def csrf_protect(request: Request) -> None:
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return

    cookie_token = request.cookies.get(CSRF_COOKIE_NAME)
    header_token = request.headers.get(CSRF_HEADER_NAME)

    if not validate_csrf_token(cookie_token, header_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF validation failed"
        )


async def origin_check(request: Request) -> None:
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return

    origin = request.headers.get("origin")
    referer = request.headers.get("referer")
    host = request.headers.get("host", "")

    if origin and host:
        from urllib.parse import urlparse
        parsed = urlparse(origin)
        if parsed.netloc and parsed.netloc != host:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Cross-origin request rejected"
            )
    elif referer and host:
        from urllib.parse import urlparse
        parsed = urlparse(referer)
        if parsed.netloc and parsed.netloc != host:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Cross-origin request rejected"
            )
