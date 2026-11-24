from quart import current_app, session, jsonify, request
from asyncmy.cursors import DictCursor
from traceback import print_exc
from functools import wraps
from async_lru import alru_cache


@alru_cache(maxsize=128, ttl=500)
async def stocks_access(inventory_id: int, brand_id: str) -> bool:
    pool = current_app.pool

    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = """
                    SELECT 1
                    FROM master_inventory AS i
                    INNER JOIN usku_record AS u
                        ON u.usku_id = i.usku_id
                    INNER JOIN warehouse AS w
                        ON w.warehouse_id = i.warehouse_id
                    WHERE i.id = %s
                    AND u.brand_id = %s
                """
                await cursor.execute(query, (inventory_id, brand_id))
                return bool(await cursor.fetchone())

        except Exception as e:
            print(e)
            print_exc()
            return "error"


def stocks_access_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        inventory_id = kwargs.get('inventory_id')
        if inventory_id is None and args:
            inventory_id = args[0]

        has_access = await stocks_access(inventory_id, session.get('brand'))
        if has_access is True:
            return await func(*args, **kwargs)
        if has_access == "error":
            raise Exception("Internal server error while checking inventory access")
        raise Exception("unauthorized", "Inventory ID does not belong to the brand login")

    return wrapper


def stocks_api_access_required(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        data = await request.get_json(silent=True) or {}
        inventory_id = data.get('inventory_id')

        if inventory_id is None:
            inventory_id = kwargs.get('inventory_id')

        try:
            has_access = await stocks_access(inventory_id, session.get('brand'))
            if has_access is True:
                return await func(*args, **kwargs)
            if has_access is False:
                return jsonify({'status': 'unauthorized', 'message': 'Invalid inventory ID for the brand'}), 401
            return jsonify({'status': 'error', 'message': 'Unexpected internal server error occurred'}), 500

        except Exception as e:
            print(e)
            print_exc()
            return jsonify({'status': 'error', 'message': 'Unexpected internal server error occurred'}), 500

    return wrapper
