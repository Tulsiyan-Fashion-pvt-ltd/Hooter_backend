from quart import current_app
from asyncmy.cursors import DictCursor
from datetime import datetime
from catalog.products.authorize import product_access_required

class Write:
            
            
    @staticmethod
    async def grn(data: dict):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into grn(inward_id, created_at)
                                values(%s, %s)
                            '''
                    values = (data.get("inward_id"), datetime.now())

                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(f"error enountered while adding grn record\n{e}")
                await connection.rollback()
                return "error"


class Fetch:
    @staticmethod
    async def grn_count(inward_id):
        """
        FETCHES THE NUMBER OF GRNS FOR THE PROVIDED INWARD ID
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select count(grn_id) as count from grn where inward_id = %s
                            '''
                    values = (inward_id, )
                    count = await cursor.execute(query, values)
                    await connection.commit()
                    return count.get("count") if count else "error"
            except Exception as e:
                print(f"error occured while fetching the grn count for the inward {inward_id}\n{e}")
                return {"error", e.args[0]}