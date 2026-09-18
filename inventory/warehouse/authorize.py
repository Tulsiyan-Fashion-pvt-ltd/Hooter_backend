from quart import current_app, session, jsonify, request
from asyncmy.cursors import DictCursor
from traceback import print_exc
from functools import wraps
from async_lru import alru_cache


@alru_cache(maxsize=128, ttl=500)
async def warehouse_access(warehouse_id: str, brand_id: str) -> bool:
    '''Checks whether the `warehouse_id` belongs to the specified brand ID or not
        
    Returns:
        - `True` -> If yes
        - `False` -> If no
    '''
    pool = current_app.pool
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT 1 FROM warehouse
                    WHERE warehouse_id = %s and brand_id = %s'''
                value = (warehouse_id, brand_id)

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

def warehouse_access_required(func):
    """Run the warehouse function if the warehouse_id belongs to the brand.
    
    Requirement:
        warehouse_id should be as the first positional argument"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        has_access = await warehouse_access(args[0], session.get('brand'))
        if has_access == True:
            return await func(*args, **kwargs)
        else:
            raise Exception("unauthorized", "Warehouse ID does not belongs to the brand login")

    return wrapper


def warehouse_api_access_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        warehouse_id = request.args.get("warehouse-id")

        if not warehouse_id:
            return await func(*args, **kwargs)
        
        try:
            has_access = await warehouse_access(warehouse_id, session.get('brand'))
            if has_access == True:
                return await func(*args, **kwargs)
            elif has_access == False:
                return jsonify({'status': 'unauthorized', "message": "Invalid Warehouse ID for the brand"}), 401
            else:
                return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500
        except Exception as e:
            print_exc
            print(e)
            return jsonify({'status': "error", "message": "Unexpected internal server error occured"}), 500

    return wrapper