from database import mysql
from datetime import datetime
from quart import current_app
from asyncmy.cursors import DictCursor

# handling the database quiries related to brands to handle brands

class Branddb:
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
        pass