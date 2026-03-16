from functools import wraps
from quart import session, jsonify
from sql_queries import userdb

def login_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if session.get('user') is None:
            return jsonify({'status': "user is not logged in"}), 401
        else:
            return await func(*args, **kwargs)
    
    return wrapper

def brand_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if session.get('brand') is None:
            return jsonify({'status': "no brand found for this user"})
        else:
            return await func(*args, **kwargs)
    
    return wrapper


def super_admin_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        user_access = await userdb.Fetch.user_access(session.get('user'))
        if (user_access == None or user_access != 'super_admin'):
            return jsonify({'status': 'access denied', 'message': 'you do not have the access kindly contact Hooter super admins'}), 401
        else:
            return await func(*args, **kwargs)
    
    return wrapper