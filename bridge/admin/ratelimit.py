import time
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from fastapi import HTTPException, Request, status


class InMemoryRateLimiter:
    def __init__(self, max_requests: int = 100, window_seconds: float = 60.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._buckets: Dict[str, List[float]] = defaultdict(list)

    def _get_key(self, request: Request) -> str:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"

        user = getattr(request.state, "user", None)
        if user:
            return f"{client_ip}:{user.username}"
        return client_ip

    def check(self, request: Request) -> None:
        key = self._get_key(request)
        now = time.time()
        window_start = now - self.window_seconds

        timestamps = self._buckets[key]
        valid = [t for t in timestamps if t > window_start]
        self._buckets[key] = valid

        if len(valid) >= self.max_requests:
            oldest = valid[0]
            retry_after = int(self.window_seconds - (now - oldest))
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Retry after {retry_after} seconds.",
                headers={"Retry-After": str(retry_after)},
            )

        self._buckets[key].append(now)

    def reset(self, key: Optional[str] = None) -> None:
        if key:
            self._buckets.pop(key, None)
        else:
            self._buckets.clear()


rate_limiter = InMemoryRateLimiter()


async def rate_limit(request: Request) -> None:
    rate_limiter.check(request)
