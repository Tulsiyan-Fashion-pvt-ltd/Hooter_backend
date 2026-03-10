from quart import current_app
from asyncmy.cursors import DictCursor

class Write:
    pass

class Fetch:
    @staticmethod
    async def count_catalogs():
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor = DictCursor) as cursor:
                try:
                    query = '''select count(usku_id) as count from usku_record'''

                    await cursor.execute(query)
                    count = await cursor.fetchone()
                    return count.get('count') if count else 0
                except Exception as e:
                    print(f"error encountered during fetching catalog counts\n{e}")
                    return ("error", "error in count_catalogs")