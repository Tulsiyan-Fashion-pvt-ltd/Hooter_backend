from quart import current_app
from asyncmy.cursors import DictCursor
from datetime import datetime
from catalog.products.authorize import product_access_required

class Write:
            
    @staticmethod
    @product_access_required
    async def add_stock(usku_id: str, stock: int):
        ...

class Fetch:
    @staticmethod
    async def inventory_stocks(brand_id: str, filter: str = "") -> list[dict[str, str| int]]:
        """Fetches the total inventory of the brand. Where it returns a list of dict objects with keys `usku_id`, `stock`
        
        Args:
            brand_id:
                Unique identifier for brands
            filter:
                filter is a string value, which takes the values as 'sellable', 'oos', 'low-stock'

        Returns:
            list of dict objects with usku_id and stock    
        """
        pool = current_app.pool
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

                    query = f'''select u.usku_id, i.stock
                                from master_inventory as i 
                                inner join
                                usku_record as u on u.usku_id = i.usku_id
                                where brand_id = %s
                                {sql_condition}
                            '''
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
            brand_id:
                Unique identifier for brands
            usku_id: Universally unique SKUID
            
        Returns:
            dict object with `usku_id`, `sku_id` and `stock` as the keys
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = f'''SELECT usku_id, stock
                                FROM master_inventory
                                WHERE usku_id = %s
                            '''
                    values = (usku_id, )

                    await cursor.execute(query, values)
                    inventory = await cursor.fetchone()
                    return inventory
            except Exception as e:
                print(f"error encountered whie fetching the inventory\n{e}")
                return "error"


    @staticmethod
    async def stock_count(brand_id: str) -> dict[str, int]:
        """
        FETCHES THE STOCK COUNT OF SELLABLE, OOS AND LOW STOCK PRODUCTS
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select
                            count(i.stock) as total, 
                            count(case when i.stock = 0 then 1 end) as oos,
                            count(case when i.stock!=0 then 1 end) as sellable,
                            count(case when i.stock <= 10 and stock >0 then 1 end) as low
                            from master_inventory as i
                            inner join usku_record as u on u.usku_id = i.usku_id
                            where u.brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    stock = await cursor.fetchone()
                    return stock if stock else {}
            except Exception as e:
                print(f"error encountered whie fetching the stock count from inventory for {brand_id}\n{e}")
                return "error"
        