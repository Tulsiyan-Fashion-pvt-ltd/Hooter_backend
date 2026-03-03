from functools import wraps
from quart import session, jsonify

def login_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if session.get('user') is None:
            return jsonify({'status': "user is not logged in"}), 401
        else:
            return await func(*args, **kwargs)
    
    return wrapper