from quart import current_app
from asyncmy.cursors import DictCursor
from datetime import datetime
from catalog.products.authorize import product_access_required
from config import _allow_max_records_per_page
from typing import Literal
from os import getenv
from dotenv import load_dotenv

load_dotenv()




class Write:
            
    @staticmethod
    @product_access_required
    async def add_stock(usku_id: str, stock: int):
        ...

class Fetch:
    @staticmethod
    async def inventory_stocks(brand_id: str, filter: str = "", type_id: str | None = None, limit=10, offset=0, warehouse_id: int | None = None) -> dict:        
        """Fetches the total inventory of the brand. Where it returns a list of dict objects with keys `usku_id`, `stock`
        Returns:
            dict with `status` and `error` if fails or
            dict with `status` and list[dict[]] if status is ok   
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    conditions = ["u.brand_id = %s"]
                    values = [brand_id]
    
                    if filter == "in-stock":
                        conditions.append("i.stock > 0")
                    elif filter == "out-of-stock":
                        conditions.append("i.stock = 0")
                    elif filter == "low-stock":
                        conditions.append("i.stock BETWEEN 1 AND 10")
    
                    if type_id:
                        conditions.append("u.type_id = %s")
                        values.append(type_id)
    
                    if warehouse_id is not None:
                        conditions.append("i.warehouse_id = %s")
                        values.append(warehouse_id)
    
                    if limit > _allow_max_records_per_page:
                        raise ValueError(
                            f"limit cannot be more than {_allow_max_records_per_page} per page"
                        )
    
                    query = f'''
                        SELECT
                            i.id,
                            i.warehouse_id,
                            u.usku_id,
                            u.sku_id,
                            i.stock,
                            u.type_name,
                            u.type_id,
                            i.updated_at,
                            c.product_title,
                            c.price * i.stock AS stock_value_at_selling_price,
                            c.compared_price * i.stock AS stock_value_at_mrp,
                            c.purchasing_cost * i.stock AS stock_value_at_purchasing_cost,
                            CONCAT('{getenv("APP_DOMAIN")}/', img_url.image_url) AS main_image,
                            CASE
                                WHEN i.stock BETWEEN 1 AND 10 THEN 'low_stock'
                                WHEN i.stock = 0 THEN 'out_of_stock'
                                WHEN i.stock > 10 THEN 'in_stock'
                            END AS status
                        FROM master_inventory AS i
                        INNER JOIN usku_record AS u
                            ON u.usku_id = i.usku_id
                        INNER JOIN catalog AS c
                            ON c.usku_id = i.usku_id
                        INNER JOIN product_images AS img
                            ON img.usku_id = i.usku_id
                            AND img.image_type = 'main'
                        INNER JOIN image_urls AS img_url
                            ON img_url.image_id = img.id
                            AND img_url.image_variation = 'webp_card'
                        WHERE {" AND ".join(conditions)}
                        LIMIT %s OFFSET %s
                    '''
    
                    values.extend([limit, offset])
    
                    await cursor.execute(query, tuple(values))
                    inventory = await cursor.fetchall()
    
                    return {'status': 'ok', 'records': inventory}
    
            except Exception as e:
                await connection.rollback()
                print(f"Error fetching inventory for {brand_id}: {e}")
                return {'error': str(e), 'status': 'failed'}



    @staticmethod
    async def product_stock(usku_id: str) -> list:
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
                    query = """
                        SELECT
                            id,
                            usku_id,
                            warehouse_id,
                            stock,
                            status,
                            updated_at
                        FROM master_inventory
                        WHERE usku_id = %s
                    """
                    
                    await cursor.execute(query, (usku_id,))
                    return await cursor.fetchall()
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
                            COUNT(i.id) AS total
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
        