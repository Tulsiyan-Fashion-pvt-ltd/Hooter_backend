from functools import wraps
from quart import session, jsonify, current_app
from asyncmy.cursors import DictCursor



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
        user_access = await user_access(session.get('user'))
        if (user_access == None or user_access != 'super_admin'):
            return jsonify({'status': 'access denied', 'message': 'you do not have the access kindly contact Hooter super admins'}), 401
        else:
            return await func(*args, **kwargs)
    
    return wrapper


async def user_access(user_id):
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cursor:
                try:
                    await cursor.execute(
                        '''
                        SELECT user_access
                        FROM user_creds
                        WHERE user_id=%s
                        ''',
                        (user_id,)
                    )
                    result = await cursor.fetchone()
                    return result["user_access"] if result else None
                except Exception as e:
                    print(f'error encountered while fetching the user access\n{e}')
                    return None