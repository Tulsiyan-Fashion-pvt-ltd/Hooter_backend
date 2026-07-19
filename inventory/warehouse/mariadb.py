from quart import current_app, json
from asyncmy.cursors import DictCursor
from datetime import datetime

class Write:
    @staticmethod
    async def warehouse(data: dict): 
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into warehouse(name, brand_id, phone, address, email)
                                values(%s, %s, %s, %s, %s)
                            '''
                    values = (data.get("name"), data.get("brand_id"), data.get("number"), data.get("address"), data.get("email"))
                    
                    await cursor.execute(query, values)
                    warehouse_id = cursor.lastrowid
                    await connection.commit()
                    return warehouse_id
            except Exception as e:
                print(f"error encountered while adding warehouse\n{e}")
                await connection.rollback()
                return "error"
            

class Fetch:
    @staticmethod
    async def warehouses(brand_id: str):
        """
        RETURNS THE WAREHOUSE OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select warehouse_id, name, phone, address, email
                            from warehouse where
                            brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)

                    warehouses= await cursor.fetchall()

                    warehouses = [{
                        "warehouse_id": warehouse.get("warehouse_id"),
                        "name": warehouse.get("name"),
                        "number": warehouse.get("phone"),
                        "email": warehouse.get("email"),
                        "address": json.loads(warehouse.get("address"))
                    } for warehouse in warehouses if warehouse]

                    return warehouses
            except Exception as e:
                print(f"error occured while fetching the warehouse of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}