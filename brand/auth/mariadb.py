from datetime import datetime
from quart import current_app
from asyncmy.cursors import DictCursor
from traceback import print_exc
from .authorize import brand_access_required
from utils.encryption import TokenEncryption

# handling the database quiries related to brands to handle brands

class Write:
    @staticmethod
    async def insert_brand(brand_data: dict, user_id: str, access: str) -> str| int:
        """Adds new brand to the database for the given user
        Args:
            - brand_data: dict object for brand containing the sql fields
            - user_id: Unique ID for the user
            - access: specific access for the user
            
            NOTE: Allowed access are `brand_admin`,  `brand_member`, `hooter_admin`, `hooter_member`
        Returns:
            - on success - literal[ok]
            - on failure - dict[literal[error], int]
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = """
                        INSERT INTO brand (
                            brand_id,
                            entity_name,
                            brand_name,
                            gtin,
                            hooter_plan,
                            address,
                            city,
                            state,
                            pincode,
                            established_year
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)
                    """

                    values = (
                        brand_data.get('brand_id'),
                        brand_data.get('entity_name'),
                        brand_data.get('brand_name'),
                        brand_data.get('gtin') if brand_data.get('gtin') and brand_data.get('gtin') != "" else None,
                        brand_data.get('plan'),
                        brand_data.get('address'),
                        brand_data.get('city'),
                        brand_data.get('state'),
                        brand_data.get('pincode'),
                        brand_data.get('estyear')
                    )
                    await cursor.execute(query, values)

                    await cursor.execute('''INSERT INTO brand_access (brand_id, user_id, user_access)
                        VALUES(%s, %s, %s)''', (brand_data.get('brand_id'), user_id, access))
                    
                    await connection.commit()
                    return 'ok'
                except Exception as e:
                    print(f'error occured while registering brand as \n{e}')
                    print_exc()
                    await connection.rollback()
                    return {"error": e.args[0]}


    @staticmethod
    async def add_poc(brand_id: str, data: dict) -> dict[str, str|int]:
        """Add new poc for the brand
        
        Args:
            brand_id: Unique ID for brand
            data: dict object for poc containing poc name, number, email and title(optional)
            
        Returns:
            Returns dict with `status` successful or failed and `poc_id` and if failed then `code` and `error` in the dict
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''INSERT INTO brand_poc(
                        brand_id, name, number, email, title)
                        VALUES (%s, %s, %s, %s, %s)'''

                    values = (  
                                brand_id, 
                                TokenEncryption.encrypt_token(data.get('name')),
                                TokenEncryption.encrypt_token(str(data.get('number'))),
                                TokenEncryption.encrypt_token(data.get('email')),
                                data.get('title')
                            )
                    await cursor.execute(query, values)
                    poc_id = cursor.lastrowid
                    await connection.commit()
                    return {'status': 'successful', poc_id: poc_id}
            except Exception as e:
                await connection.rollback()
                print(f"Error occured while adding the brand poc, code: a9ff7ec4", e)
                print_exc()
                return {'status': 'failed', 'code': 'a9ff7ec4', 'error': e.args[0]}



    @staticmethod
    async def map_user_brand(brand_id, user_id, access: str) -> bool:
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = """
                        INSERT INTO brand_access (brand_id, user_id, user_access)
                        VALUES (%s, %s, %s)
                    """
                    await cursor.execute(query, (brand_id, user_id, access))
                    await connection.commit()
                    return 'ok'
                except Exception as e:
                    print(f'error occured while mapping user to the brand as \n {e}')
                    await connection.rollback()
                    return e.args[0]

    @staticmethod
    @brand_access_required
    async def update_poc(brand_id: str, data: dict) -> str| dict[str, int]:
        '''Updates the POC for the given brand_id if the brand belongs to the user else raise an exception
        
        Args:
            - brand_id - Unique ID for brand
            - data - dict with key and value pair for the attribute for update
        
        Returns:
            - on success - literal[ok]
            - on failure - dict[literal[error], int]
        '''
        pool = current_app.pool
        attrs = []
        values = []

        '''extracting key and value pairs'''
        for attr in data.keys():
            attrs.append(f"{attr} = %s")

            if attr in ('name', 'number', 'email', 'title'):
                values.append(TokenEncryption.encrypt_token(data.get(attr)))
            else:
                values.append(data.get(attr))

        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = f"""
                                UPDATE brand_poc
                                SET {(', ').join(attrs)}
                                WHERE brand_id = %s
                            """
                    
                    values.append(brand_id)

                    await cursor.execute(query, values)
                    await connection.commit()
                    return {'status': 'successful'}
                
            except Exception as e:
                await connection.rollback()
                print('code: 70fc35f4', e)
                return {'status': 'failed', 'code': '70fc35f4', 'error': e.args[0]} 


                
class Fetch:
    
    @staticmethod
    async def brand_access(user_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''SELECT brand.brand_name, access.brand_id 
                                FROM brand_access as access
                                inner join brand as brand
                                on brand.brand_id = access.brand_id
                                Where access.user_id = %s'''
                    
                    await cursor.execute(query, (user_id, ))

                    brand_access = await cursor.fetchall()
                    await connection.commit()
                    return brand_access
                except Exception as e:
                    await connection.rollback()
                    print(f"error occured during fetching brand access of the user {user_id}\n{e}")
                    return None
                
    
    @staticmethod
    async def check_brand_id(brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''SELECT 1 FROM brand WHERE brand_id = %s'''

                    await cursor.execute(query, (brand_id, ))
                    result = await cursor.fetchone() 
                    await connection.commit()
                    return "available" if result and result.get('1') == 1 else "unavailable"
                except Exception as e:
                    await connection.rollback()
                    print(f"error during checking the brand availability", e)
                    return ("error", "unable to fulfill the request")



    # check name of brand from the brand_id
    @staticmethod
    async def brand_name_by_id(brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''SELECT brand_name FROM brand WHERE brand_id = %s'''
                    await cursor.execute(query, (brand_id, ))

                    result = await cursor.fetchone()
                    brand_name = result.get('brand_name')
                    # print(brand_name)
                    await connection.commit()
                    return brand_name
                except Exception as e:
                    await connection.rollback()
                    print(f"error during fetching the brand_name from the brand table in brand_name_by_id\n{e}")
                    return ("error", "unable to fulfill the request")