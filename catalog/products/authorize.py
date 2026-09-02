from quart import current_app, session, jsonify
from asyncmy.cursors import DictCursor
from traceback import print_exc
from functools import wraps
from async_lru import alru_cache

@alru_cache(maxsize=128, ttl=500)
async def product_access(usku_id: str, brand_id: str) -> bool:
    '''Checks whether the `usku_id` belongs to the specified brand ID or not
    
    Args:
        usku_id: globally unique identifier for the product or stock
        brand_id: Unique identifier for the brand
        
    Returns:
        - `True` -> If yes
        - `False` -> If no
    '''
    pool = current_app.pool
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT 1 FROM usku_record
                    WHERE usku_id = %s and brand_id = %s'''
                value = (usku_id, brand_id)

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



def product_access_required(func):
    """Run the product function if the usku_id belongs to the brand.
    
    Requirement:
        usku_id should be as the first positional argument"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        has_access = await product_access(args[0], session.get('brand'))
        if has_access == True:
            return await func(*args, **kwargs)
        else:
            raise Exception("unauthorized", "USKU ID does not belongs to the brand login")

    return wrapper


def product_api_access_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if not kwargs.get('usku_id'):
            return await func(*args, **kwargs)
        
        try:
            has_access = await product_access(kwargs.get('usku_id'), session.get('brand'))
            if has_access == True:
                return await func(*args, **kwargs)
            elif has_access == False:
                return jsonify({'status': 'unauthorized', "message": "Invalid USKU ID for the brand"}), 401
            else:
                return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500
        except Exception as e:
            print(e)
            print_exc
            return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500

    return wrapper