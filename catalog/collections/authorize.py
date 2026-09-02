from quart import current_app, session, jsonify
from asyncmy.cursors import DictCursor
from traceback import print_exc
from functools import wraps
from async_lru import alru_cache


@alru_cache(maxsize=128, ttl=500)
async def collection_access(collection_id: int, brand_id: str) -> bool:
    """Checks the collection access for the given `collection_id`
    if the collection belongs to the brand or not.
    
    Args:
        collection_id: unique id for the collections
        brand_id: unique id for the brands
        
    Returns:
        - True -> if collection belongs to brand
        - False -> if collection doens't belong to brand"""
    pool = current_app.pool
    if not pool:
        raise Exception("pool", "asyncmy connection pool not available")

    async with pool.acquire() as connetion:
        try:
            async with connetion.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT 1 as available FROM collection_records
                WHERE brand_id = %s AND collection_id = %s'''
                value = (brand_id, collection_id)

                await cursor.execute(query, value)
                is_available = await cursor.fetchone()

                if not is_available:
                    return False
                else:
                    return True
        except Exception as e:
            print(e)
            print_exc()
            return "error"




def colletion_access_required(func):
    """Checks the access required for collection functions
    takes `collection_id` in first positional arguments
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        authorize = await collection_access(args[0], session.get('brand'))
        if not authorize:
            return False
        else:
            return await func(*args, **kwargs)

    return wrapper




def collection_api_access_required(func):
    """Checks the access required for collection APIs
    takes `collection_id` in as keyword argument
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            authorize = await collection_access(kwargs.get("collection_id"), session.get("brand")) # args[0] expects collection_id
        except Exception as e:
            if e.args[0] == "pool":
                return jsonify({"status": "failed", "message": "unexpected internal server occured"}), 500
            else:
                print(e)
                print_exc
                return jsonify({"status": "failed", "message": "internal server error"}), 500

        if authorize:
            return await func(*args, **kwargs)
        else:
            return jsonify({'status': "unauthorized", "message": 'Incorrect collection ID for the brand'}), 401

    return wrapper