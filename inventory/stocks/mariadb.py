from quart import current_app, json
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
    async def inventory(brand_id: str, filter: str = "", usku_id: str = None):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    sql_condition = ""
                    if filter == "sellable":
                        sql_condition = "and c.product_stock != 0"
                    elif filter == "oos":
                        sql_condition = "and c.product_stock = 0"
                    elif filter == "low-stock":
                        sql_condition = "and c.product_stock <= 10 and c.product_stock > 0"   

                    query = f'''select img.image_url, u.usku_id, u.sku_id, c.product_title, n.product_name as product_type, c.product_stock
                                from usku_record as u
                                inner join catalog as c on u.usku_id = c.usku_id
                                inner join niche_products as n on u.product_type_id = n.type_id
                                inner join images as img on u.usku_id = img.usku_id
                                where u.brand_id = %s and u.status="completed" and img.image_type = "front"
                                and {"u.usku_id = %s" if usku_id else "1=1"}
                                {sql_condition}
                            '''
                    # print(query)
                    values = (brand_id, usku_id) if usku_id else (brand_id, )

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
                            count(c.product_stock) as total, 
                            count(case when c.product_stock = 0 then 1 end) as oos,
                            count(case when c.product_stock!=0 then 1 end) as sellable,
                            count(case when c.product_stock <= 10 and c.product_stock >0 then 1 end) as low
                            from catalog as c
                            inner join usku_record as u on c.usku_id = u.usku_id
                            where u.brand_id = %s and u.status="completed"
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