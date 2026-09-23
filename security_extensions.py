"""Security_extensions is an extenion of application

MIDDLEWARES:
    - quart-rate-limiter
    - csrf-tokens

    
RATE LIMTER:
    Rate limiter uses the `quart-rate-limiter` library to implement rate limiting on the application

    The application has default rate limit of 300 reqs per minute.
    This can be overwrite by using limiter decorator

    example:
    from quart_rate_limiter import rate_limit, RateLimit, timedelta

    @rate_limit(5, timedelta(minutes=15))
    async def api_func():
        ...
    
CSRF TOKEN:
    CSRF tokens are used to verify the authenticity for state changing APIs.
    This library provides an enpoint '/security/csrf and validate_csrf wrapper for APIs.

    **CSRF TOKEN ARE STRICTLY TO BE IMPLEMENTED BY ALL DELETE, POST AND PUT METHODS OR ANY API WHICH CHANGES
    THE STATES IN THE SYSTEM**

    PROCESS:
        1.  get_csrf_token is called by api call and the generated token then saved by the client in the session
            and in the variable in js or redux if using react.
        
        2.  Send the CSRF token inside the header using the key {`X-CSRFToken`: csrftoken}

        3.  Verify the token using `validate_csrf` wrapper
            
            EXAMPLE:
                from security_extentions import validate_csrf

                @app.post('/resource')
                @validate_csrf
                @login_required
                async def create_resource():
                    ...

        NOTE:  MIDDLEWARE RETURNS 403 FORBIDDEN 
"""
from quart import request, g, Blueprint, jsonify, session, request, Response
from quart_rate_limiter import RateLimiter, RateLimit
from datetime import timedelta
import secrets
import hmac
from functools import wraps

security = Blueprint('security', __name__, url_prefix = "/security")



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



'''CSRF TOKEN'''

'''API'''
@security.get('/csrf')
async def get_csrf_token() -> Response:
    '''Gerate CSRF token for state changing api verifications'''

    if "csrf_token" not in session:
        session['csrf_token'] = secrets.token_hex(32)

    return jsonify({"csrf_token": session.get('csrf_token')})



async def verify_csrf() -> bool:
    '''Verfy csrf token'''
    token = request.headers.get("X-CSRF-Token", "")

    if token == "" or not session.get('csrf_token'):
        return False

    return hmac.compare_digest(
        token,
        session.get("csrf_token", "")
    )


def validate_csrf(func: function) -> function | Response:
    '''Wrapper to verify the CSRF token'''

    @wraps(func)
    async def wrapper(*args, **kwargs):
        if await verify_csrf():
            return await func(*args, **kwargs)

        else:
            return jsonify({'status': 'failed', 'message': 'Forbidden'}), 403

    return wrapper