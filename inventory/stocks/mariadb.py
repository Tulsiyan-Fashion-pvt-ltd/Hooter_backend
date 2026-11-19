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

    
    @staticmethod
    async def adjust_stock(inventory_id: int, quantity: int):
        pool = current_app.pool
    
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    await connection.begin()
    
                    # Update stock (positive = add, negative = remove)
                    query = """
                        UPDATE master_inventory
                        SET stock = stock + %s
                        WHERE id = %s
                        AND status = 'active'
                        AND stock + %s >= 0
                    """
                    await cursor.execute(query, (quantity, inventory_id, quantity))
                    if cursor.rowcount == 0:
                        await connection.rollback()
                        return False
    
                    # Determine movement type
                    movement_type = 'added' if quantity > 0 else 'removed'
    
                    # Record stock movement
                    query = """
                        INSERT INTO master_inventory_movements
                        (inventory_id, qty_change, movement_type, reason)
                        VALUES (%s, %s, %s, 'adjustment')
                    """
                    await cursor.execute(query, (inventory_id, abs(quantity), movement_type))   
                    await connection.commit()
                    return True
    
            except Exception as e:
                await connection.rollback()
                print(f"Error adjusting inventory stock: {e}")
                return "error"
    
    
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
    
                    having = ""

                    if filter == "in-stock":
                        having = "HAVING SUM(i.stock) > 0"
                    elif filter == "out-of-stock":
                        having = "HAVING SUM(i.stock) = 0"
                    elif filter == "low-stock":
                        having = "HAVING SUM(i.stock) BETWEEN 1 AND 10"
    
                    if type_id:
                        conditions.append("u.type_id = %s")
                        values.append(type_id)
    
                    if warehouse_id is not None:
                        conditions.append("i.warehouse_id = %s")
                        values.append(warehouse_id)
    
                    if limit > _allow_max_records_per_page:
                        raise ValueError(f"limit cannot be more than {_allow_max_records_per_page} per page")
    
                    query = f"""
                        SELECT
                            u.usku_id,
                            u.sku_id,
                            SUM(i.stock) AS stock,
                            u.type_name,
                            u.type_id,
                            MAX(i.updated_at) AS updated_at,
                            c.product_title,
                            c.price * SUM(i.stock) AS stock_value_at_selling_price,
                            c.compared_price * SUM(i.stock) AS stock_value_at_mrp,
                            c.purchasing_cost * SUM(i.stock) AS stock_value_at_purchasing_cost,
                            CONCAT('{getenv("APP_DOMAIN")}/', img_url.image_url) AS main_image,
                            CASE
                                WHEN SUM(i.stock) BETWEEN 1 AND 10 THEN 'low_stock'
                                WHEN SUM(i.stock) = 0 THEN 'out_of_stock'
                                WHEN SUM(i.stock) > 10 THEN 'in_stock'
                            END AS status
                        FROM master_inventory AS i
                        INNER JOIN usku_record AS u
                            ON u.usku_id = i.usku_id
                        INNER JOIN catalog AS c
                            ON c.usku_id = i.usku_id
                        LEFT JOIN product_images AS img
                            ON img.usku_id = i.usku_id
                            AND img.image_type = 'main'
                        LEFT JOIN image_urls AS img_url
                            ON img_url.image_id = img.id
                            AND img_url.image_variation = 'webp_card'
                        WHERE {" AND ".join(conditions)}
                        GROUP BY
                            u.usku_id,
                            u.sku_id,
                            u.type_name,
                            u.type_id,
                            c.product_title,
                            c.price,
                            c.compared_price,
                            c.purchasing_cost,
                            img_url.image_url
                        {having}
                        LIMIT %s OFFSET %s
                    """
    
                    values.extend([limit, offset])   
                    await cursor.execute(query, tuple(values))
                    inventory = await cursor.fetchall()
    
                    return {'status': 'ok', 'records': inventory}
    
            except Exception as e:
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
                    query = """
                        SELECT
                            COUNT(*) AS total,
                            COUNT(CASE WHEN inventory.stock = 0 THEN 1 END) AS out_of_stock,
                            COUNT(CASE WHEN inventory.stock > 0 THEN 1 END) AS in_stock,
                            COUNT(CASE WHEN inventory.stock BETWEEN 1 AND 10 THEN 1 END) AS low_stock,
                            SUM(c.purchasing_cost * inventory.stock) AS stock_value_at_purchasing_cost,
                            SUM(c.compared_price * inventory.stock) AS stock_value_at_mrp,
                            SUM(c.price * inventory.stock) AS stock_value_at_selling_price
                        FROM (
                            SELECT
                                i.usku_id,
                                SUM(i.stock) AS stock
                            FROM master_inventory AS i
                            INNER JOIN usku_record AS u
                                ON u.usku_id = i.usku_id
                            WHERE u.brand_id = %s
                            GROUP BY i.usku_id
                        ) AS inventory
                        INNER JOIN catalog AS c
                            ON c.usku_id = inventory.usku_id
                    """
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    stock = await cursor.fetchone()
                    return stock if stock else {}
            except Exception as e:
                print(f"error encountered whie fetching the stock count from inventory for {brand_id}\n{e}")
                return "error"
        