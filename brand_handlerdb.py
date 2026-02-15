from database import mysql
from datetime import datetime

# handling the database quiries related to brands to handle brands

class Branddb:
    class Write:
        @staticmethod
        def insert_brand(brand_id, user_id, brand_data):
            cursor = mysql.connection.cursor()
            try:
                query = """
                    INSERT INTO brand (
                        brand_id,
                        entity_name,
                        brand_name,
                        brand_niche,
                        gstin,
                        hooter_plan,
                        registered_address,
                        establishment_year,
                        poc,
                        created_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cursor.execute(query, (
                    brand_id,
                    brand_data.get('entity_name'),
                    brand_data.get('brand_name'),
                    brand_data.get('niche'),
                    brand_data.get('gstin'),
                    brand_data.get('plan'),
                    brand_data.get('address'),
                    brand_data.get('estYear'),
                    user_id,
                    datetime.datetime.now().date()
                ))
                mysql.connection.commit()
            except Exception as e:
                print(f'error occured while registering brand as \n{e}')
                mysql.connection.rollback()
                return 'failed'
            finally:
                cursor.close()
            return 'ok'
    
        @staticmethod
        def map_user_brand(user_id, brand_id):
            cursor = mysql.connection.cursor()
            try:
                query = """
                    INSERT INTO brand_access (brand_id, user_id)
                    VALUES (%s,%s)
                """
                cursor.execute(query, (brand_id, user_id))
                mysql.connection.commit()
            except Exception as e:
                print(f'error occured while mapping user to the brand as \n {e}')
                mysql.connection.rollback()
                raise
            finally:
                cursor.close()
    
    
    class Fetch:
        pass