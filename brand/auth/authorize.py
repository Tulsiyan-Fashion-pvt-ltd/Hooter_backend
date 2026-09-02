from quart import current_app, session, jsonify
from asyncmy.cursors import DictCursor
from traceback import print_exc
from functools import wraps
from async_lru import alru_cache

@alru_cache(maxsize=128, ttl=500)
async def brand_access(brand_id: str, user_id: str) -> bool:
    '''Checks whether the `usku_id` belongs to the specified brand ID or not
    
    Args:
        usku_id: globally unique identifier for the brand or stock
        brand_id: Unique identifier for the brand
        
    Returns:
        - `True` -> If yes
        - `False` -> If no
    '''
    pool = current_app.pool
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT 1 FROM brand_access
                    WHERE user_id = %s and brand_id = %s'''
                value = (user_id, brand_id)

                await cursor.execute(query, value)
                belongs = await cursor.fetchone()

                if belongs:
                    return True
                else:
                    return False
        except Exception as e:
            print(e)
            print_exc
            return "error"



def brand_access_required(func):
    """Run the brand function if the `brand_id` belongs to the user.
    
    Requirement:
        `brand_id` should be as the first positional argument"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        has_access = await brand_access(args[0], session.get('user'))
        if has_access == True:
            return await func(*args, **kwargs)
        else:
            raise Exception("unauthorized", "brand_id does not belongs to the user")

    return wrapper


def brand_api_access_required(func):
    """Run the brand api if the `brand_id` belongs to the user.
        
    Requirement:
        `brand_id` should a kwarg"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        '''If the api doesn't pass brand_id then no need to check'''
        if not kwargs.get('brand_id'):
            return await func(*args, **kwargs)
        
        try:
            has_access = await brand_access(kwargs.get('brand_id'), session.get('user'))
            if has_access == True:
                return await func(*args, **kwargs)
            elif has_access == False:
                return jsonify({'status': 'unauthorized', "message": "Invalid brand_id for the user"}), 401
            else:
                return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500
        except Exception as e:
            print(e)
            print_exc
            return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500

    return wrapper