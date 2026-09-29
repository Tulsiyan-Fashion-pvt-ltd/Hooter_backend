from quart import current_app, json
from asyncmy.cursors import DictCursor
from traceback import print_exc

class Write:
    @staticmethod
    async def warehouse(brand_id: str, data: dict): 
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into warehouse(name, alias, brand_id, phone_number, email, address, city, state, pincode)
                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            '''
                    values = (data.get("name"), data.get("alias"), brand_id, data.get("phone_number"), data.get("email"),
                               data.get("address"), data.get('city'), data.get('state'), data.get('pincode'))
                    
                    await cursor.execute(query, values)
                    warehouse_id = cursor.lastrowid
                    await connection.commit()
                    return warehouse_id
            except Exception as e:
                print(f"error encountered while adding warehouse\n{e}")
                await connection.rollback()
                return "error"


    @staticmethod
    async def switch_warehouse_to_inactive(warehouse_id:str): 
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                UPDATE warehouse
                                SET status='inactive'
                                WHERE warehoue_id = %s
                            '''
                    
                    await cursor.execute(query, (warehouse_id, ))
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(f"error encountered while deleting warehouse\n{e}")
                await connection.rollback()
                return "error"
        

    @staticmethod
    async def update_warehouse(warehouse_id: str,data: dict) -> str | dict[str, int]:
        '''Updating the specified warehouse data
        Args:
          - warehouse_id - unique id for warehouse
          - data - attribute and value for warehouse as key:value pair
          
        Returns:
          - on success - literal[ok]
          - on failure - dict[literal[error], int]
        '''
        pool = current_app.pool
        async with pool.acquire() as connection:
            query_attr = [] 
            values = []
            if data!= None and type(data) == dict:
                for key in data.keys():
                    query_attr.append(f"{key} = %s")
                    values.append(data.get(key))
            
            try:
                async with connection.cursor() as cursor:
                    query = f'''update warehouse
                    set {",".join(query_attr)}
                    where warehouse_id = %s'''

                    print(query)
                    await cursor.execute(query, (*values, warehouse_id))
                    await connection.commit()
                    return "ok"

            except Exception as e:
                print(e)
                print_exc()
                await connection.rollback()
                return {"error" : e.args[0]}
        
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
                            select warehouse_id, alias, name, phone_number, address, city, state, pincode, email, status
                            from warehouse where
                            brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)

                    warehouses= await cursor.fetchall()

                    return warehouses
            except Exception as e:
                print(f"error occured while fetching the warehouse of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}


    @staticmethod
    async def warehouse(warehouse_id: str) -> dict[str: str | int] | dict[str, int]:
        """
        RETURNS THE PARTICULAR WAREHOUSE

        Args:
          - warehouse_id : unique id for warehouse
        Returns:
          - on success : dict of warehouse object
          - on failure : dict[literal[error], int]
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select warehouse_id, alias, name, phone_number, email, address, state, city, pincode, status
                            from warehouse where
                            warehouse_id = %s
                            '''
                    values = (warehouse_id, )
                    await cursor.execute(query, values)

                    warehouse= await cursor.fetchone()
                    return warehouse
            except Exception as e:
                print(f"error occured while fetching the warehouse of the brand for the warehouseid=>{warehouse_id}\n{e}")
                print_exc()
                return {"error": e.args[0]}