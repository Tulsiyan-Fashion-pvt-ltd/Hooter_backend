from quart import current_app, session
from asyncmy.cursors import DictCursor
from datetime import datetime

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
    async def inventory(brand_id: str, filter: str = ""):
        pool = current_app.pool
        brand_id = session.get('brand')
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    sql_condition = ""
                    if filter == "sellable":
                        sql_condition = "and stock != 0"
                    elif filter == "oos":
                        sql_condition = "and stock = 0"
                    elif filter == "low-stock":
                        sql_condition = "and stock <= 10 and stock > 0"   

                    query = f'''select usku_id, sku_id, stock
                                from usku_record
                                where brand_id = %s and status="completed"
                                {sql_condition}
                            '''
                    # print(query)
                    values = (brand_id, )

                    await cursor.execute(query, values)
                    inventory = await cursor.fetchall()
                    return inventory
            except Exception as e:
                print(f"error encountered whie fetching the inventory for {brand_id}\n{e}")
                return "error"

    @staticmethod
    async def product_stock(usku_id: str) -> dict[str, str| int]:
        """Get product stock for the usku_id for the brand session
        
        Args:
            usku_id: Universally unique SKUID
            
        Returns:
            dict object with `usku_id`, `sku_id` and `stock` as the keys
        """
        pool = current_app.pool
        brand_id = session.get('brand')
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = f'''SELECT usku_id, stock
                                FROM master_inventory
                                WHERE usku_id = %s
                            '''
                    values = (usku_id, )

                    await cursor.execute(query, values)
                    inventory = await cursor.fetchall()
                    return inventory
            except Exception as e:
                print(f"error encountered whie fetching the inventory for {brand_id}\n{e}")
                return "error"


    @staticmethod
    async def stock_count(brand_id: str):
        """
        FETCHES THE STOCK COUNT OF SELLABLE, OOS AND LOW STOCK PRODUCTS
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select
                            count(stock) as total, 
                            count(case when stock = 0 then 1 end) as oos,
                            count(case when stock!=0 then 1 end) as sellable,
                            count(case when stock <= 10 and stock >0 then 1 end) as low
                            from catalog as c
                            inner join usku_record as u on c.usku_id = usku_id
                            where brand_id = %s and status="completed"
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    stock = await cursor.fetchone()
                    return stock if stock else {}
            except Exception as e:
                print(f"error encountered whie fetching the stock count from inventory for {brand_id}\n{e}")
                return "error"
        

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