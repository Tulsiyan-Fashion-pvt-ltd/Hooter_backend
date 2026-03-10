from database_connection import mysql
from datetime import datetime
from quart import current_app
from asyncmy.cursors import DictCursor

# handling the database quiries related to brands to handle brands

class Write:
    @staticmethod
    async def insert_brand(brand_id, user_id, brand_data):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = """
                        INSERT INTO brand (
                            brand_id,
                            entity_name,
                            brand_name,
                            gstin,
                            hooter_plan,
                            registered_address,
                            established_year,
                            poc,
                            created_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    await cursor.execute(query, (
                        brand_id,
                        brand_data.get('entity_name'),
                        brand_data.get('brand_name'),
                        brand_data.get('gstin'),
                        brand_data.get('plan'),
                        brand_data.get('address'),
                        brand_data.get('estyear'),
                        user_id,
                        datetime.now().date()
                    ))
                    await cursor.execute('''INSERT INTO brand_access (brand_id, user_id)
                        VALUES(%s, %s)''', (brand_id, user_id))
                    await connection.commit()
                except Exception as e:
                    print(f'error occured while registering brand as \n{e}')
                    await connection.rollback()
                    return 'failed'
                return 'ok'
    @staticmethod
    async def map_user_brand(user_id, brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = """
                        INSERT INTO brand_access (brand_id, user_id)
                        VALUES (%s,%s)
                    """
                    await cursor.execute(query, (brand_id, user_id))
                    await connection.commit()
                except Exception as e:
                    print(f'error occured while mapping user to the brand as \n {e}')
                    await connection.rollback()
                    raise

class Fetch:
    
    @staticmethod
    async def brand_access(user_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''SELECT brand_id FROM brand_access
                                Where user_id = %s'''
                    
                    await cursor.execute(query, (user_id, ))

                    brand_access = await cursor.fetchall()
                    return brand_access
                except Exception as e:
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
                    return "available" if result and result.get('1') == 1 else "unavailable"
                except Exception as e:
                    print(f"error during checking the brand availability")
                    return ("error", "unable to fulfill the request")


    # this function only tells if the brand has uploaded a single catalog or not            
    @staticmethod
    async def is_exists_catalog(brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''Select 1 from usku_record where brand_id = %s'''

                    await cursor.execute(query, (brand_id, ))
                    catalog_available = await cursor.fetchone()
                    return True if catalog_available and catalog_available.get('1') else False
                except Exception as e:
                    print(f"error occured while fetching the catalog on is_exists_catalog function\n{e}")
                    return ("error", "could not fetch the availability from the usku_record")


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
                except Exception as e:
                    print(f"error during fetching the brand_name from the brand table in brand_name_by_id\n{e}")
                    return ("error", "unable to fulfill the request")