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

                    query = f'''SELECT 
                                    u.usku_id, u.sku_id, i.stock,  u.type_name,
                                    i.updated_at, c.product_title, 
                                    SUM(c.price * i.stock) AS stock_value_at_selling_price,
                                    SUM(c.compared_price * i.stock) AS stock_value_at_mrp,
                                    SUM(c.purchasing_cost  * i.stock) AS stock_value_at_purchasing_cost,
                                    img_url.image_url as main_image,
                                    CASE 
                                        WHEN i.stock <= 10 AND i.stock >=1 THEN 'low_stock' 
                                        WHEN i.stock = 0 THEN 'out_of_stock' 
                                        WHEN i.stock > 0 THEN 'in_stock'
                                    end as status
                                FROM master_inventory as i 
                                INNER JOIN usku_record as u 
                                    ON u.usku_id = i.usku_id
                                INNER JOIN catalog as c
                                    ON  c.usku_id = i.usku_id
                                INNER JOIN product_images as img
                                    ON img.usku_id = i.usku_id
                                    AND img.image_type="main"
                                INNER JOIN image_urls as img_url
                                    ON img_url.image_id = img.id
                                    AND image_variation='webp_card'
                                WHERE brand_id = %s
                                {sql_condition}
                                GROUP BY 
                                u.usku_id, 
                                u.sku_id, 
                                i.stock,  
                                u.type_name,
                                i.updated_at, 
                                c.product_title,
                                img_url.image_url
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    inventory = await cursor.fetchall()
                    await connection.commit()
                    return inventory
            except Exception as e:
                await connection.rollback()
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
                    await connection.commit()
                    return inventory
            except Exception as e:
                await connection.rollback()
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
                            count(case when i.stock = 0 then 1 end) as out_of_stock,
                            count(case when i.stock!=0 then 1 end) as in_stock,
                            count(case when i.stock <= 10 and stock >0 then 1 end) as low_stock,
                            sum(c.purchasing_cost * i.stock) as stock_value_at_purchasing_cost,
                            sum(c.compared_price * i.stock) as stock_value_at_mrp,
                            sum(c.price * i.stock) as stock_value_at_selling_price
                            from master_inventory as i
                            inner join usku_record as u on u.usku_id = i.usku_id
                            inner join catalog as c on c.usku_id = i.usku_id
                            where u.brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    stock = await cursor.fetchone()
                    await connection.commit()
                    return stock if stock else {}
            except Exception as e:
                await connection.rollback()
                print(f"error encountered whie fetching the stock count from inventory for {brand_id}\n{e}")
                return "error"
        