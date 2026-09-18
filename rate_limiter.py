"""Rate limiter uses the `quart-rate-limiter` liabrary to implement rate limiting on the application

The application has default rate limit of 300 reqs per minute.
This can be overwrite by using limiter decorator

example:
from quart_rate_limiter import rate_limit, RateLimit, timedelta

@rate_limit(5, timedelta(minutes=15))
async def api_func():
    ...
"""

from quart import request, g
from quart_rate_limiter import RateLimiter, RateLimit
from datetime import timedelta


async def rate_limit_key():
    """Generates key based upon the user session if not then client IP address"""
    if getattr(g, "user_id", None):
        return f"brand:{g.user_id}"      # logged-in user

    return f"ip:{request.headers.get('CF-Connecting-IP', request.remote_addr)}"




limiter = RateLimiter(key_function=rate_limit_key,
                    default_limits=[
                        RateLimit(300, timedelta(minutes=1))
                    ]
            )