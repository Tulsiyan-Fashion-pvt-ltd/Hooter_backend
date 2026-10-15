from quart import current_app, session, jsonify
from asyncmy.cursors import DictCursor
from traceback import print_exc
from functools import wraps
from async_lru import alru_cache

@alru_cache(maxsize=128, ttl=500)
async def active_shopify_store_access(store_id: str, brand_id: str) -> bool:
    '''Checks whether the `store_id` belongs to the specified brand ID or not
    
    Args:
        store_id: globally unique identifier for the brand or stock
        brand_id: Unique identifier for the brand
        
    Returns:
        - `True` -> If yes
        - `False` -> If no
    '''
    pool = current_app.pool
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT 1 FROM shopify_stores
                    WHERE brand_id = %s and store_id = %s and status="active"
                    '''
                value = (brand_id, store_id)

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


@alru_cache(maxsize=128, ttl=500)
async def shopify_store_access(store_id: str, brand_id: str) -> bool:
    '''Checks whether the `store_id` belongs to the specified brand ID or not
    
    Args:
        store_id: globally unique identifier for the brand or stock
        brand_id: Unique identifier for the brand
        
    Returns:
        - `True` -> If yes
        - `False` -> If no
    '''
    pool = current_app.pool
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT 1 FROM shopify_stores
                    WHERE brand_id = %s and store_id = %s
                    '''
                value = (brand_id, store_id)

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



def active_shopify_store_access_required(func):
    """Run the shopify_store function if the `active store_id` belongs to the user.
    
    Requirement:
        `store_id` should be as the first positional argument"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        has_access = await active_shopify_store_access(args[0], session.get('brand'))
        if has_access == True:
            return await func(*args, **kwargs)
        else:
            raise Exception("unauthorized", "store_id does not belongs to the user")

    return wrapper


def shopify_store_access_required(func):
    """Run the shopify_store function if the `store_id` belongs to the user active or inactive.
    
    Requirement:
        `store_id` should be as the first positional argument"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        has_access = await active_shopify_store_access(args[0], session.get('brand'))
        if has_access == True:
            return await func(*args, **kwargs)
        else:
            raise Exception("unauthorized", "store_id does not belongs to the user")

    return wrapper




def shopify_store_api_access_required(func):
    """Run the brand_shopify api if the `store_id` belongs to the user.
        
    Requirement:
        `store_id` should a kwarg
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        '''If the api doesn't pass store_id then no need to check'''
        if not kwargs.get('store_id'):
            return await func(*args, **kwargs)
        
        try:
            has_access = await shopify_store_access(kwargs.get('store_id'), session.get('user'))
            if has_access == True:
                return await func(*args, **kwargs)
            elif has_access == False:
                return jsonify({'status': 'unauthorized', "message": "Invalid store_id for the user"}), 401
            else:
                return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500
        except Exception as e:
            print(e)
            print_exc
            return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500

    return wrapper



def active_shopify_store_api_access_required(func):
    """Run the brand shopify store api if the `active store_id` belongs to the user.
        
    Requirement:
        `store_id` should a kwarg
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        '''If the api doesn't pass store_id then no need to check'''
        if not kwargs.get('store_id'):
            return await func(*args, **kwargs)
        
        try:
            has_access = await active_shopify_store_access(kwargs.get('store_id'), session.get('user'))
            if has_access == True:
                return await func(*args, **kwargs)
            elif has_access == False:
                return jsonify({'status': 'unauthorized', "message": "Invalid store_id for the user"}), 401
            else:
                return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500
        except Exception as e:
            print(e)
            print_exc
            return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500

    return wrapper