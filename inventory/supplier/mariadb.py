from quart import current_app, json
from asyncmy.cursors import DictCursor
from datetime import datetime


class Write:
    @staticmethod
    async def supplier(data: dict): 
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into supplier(name, brand_id, contact_number, address, email, created_at)
                                values(%s, %s, %s, %s, %s, %s)
                            '''
                    values = (data.get("name"), data.get("brand_id"), data.get("number"), data.get("address"), 
                              data.get("email"), datetime.now())
                    
                    await cursor.execute(query, values)
                    supplier_id = cursor.lastrowid

                    await connection.commit()
                    return supplier_id
            except Exception as e:
                print(f"error encountered while adding supplier\n{e}")
                await connection.rollback()
                return "error"
            

class Fetch:
    @staticmethod
    async def suppliers(brand_id: str):
        """
        RETURNS THE SUPPLIERS OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select supplier_id, name, contact_number, address, email
                            from supplier where
                            brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)

                    suppliers = await cursor.fetchall()

                    suppliers = [{
                        "supplier_id": supplier.get("supplier_id"),
                        "name": supplier.get("name"),
                        "number": supplier.get("contact_number"),
                        "email": supplier.get("email"),
                        "address": json.loads(supplier.get("address"))
                    } for supplier in suppliers if suppliers]

                    return suppliers
            except Exception as e:
                print(f"error occured while fetching the suppliers of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}
            
    
    @staticmethod
    async def supplier(brand_id: str, supplier_id: str):
        """
        RETURNS THE SUPPLIERS OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select name, contact_number, address, email
                            from supplier where
                            brand_id = %s and supplier_id = %s
                            '''
                    values = (brand_id, supplier_id)
                    await cursor.execute(query, values)

                    supplier = await cursor.fetchone()

                    supplier = {
                        "name": supplier.get("name"),
                        "number": supplier.get("contact_number"),
                        "email": supplier.get("email"),
                        "address": json.loads(supplier.get("address"))
                    }

                    return supplier
            except Exception as e:
                print(f"error occured while fetching the supplier of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}