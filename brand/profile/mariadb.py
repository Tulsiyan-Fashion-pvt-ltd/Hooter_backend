from quart import current_app
from typing import Literal
from datetime import datetime
from asyncmy.cursors import DictCursor


class Write:
    @staticmethod
    async def profile_img_key(brand_id: str, key: str) -> dict[str, str|int]:
        """Uploads the profile image key to the db
        
        Args:
            - brand_id
                Unique brand ID
            
            - key
                S3 object key

        Returns:
            - on success
                {"status": "ok"}
            - on error
                {'status': 'failed', 'code': 'ed37ed2d', 'error': `sql error status`}
                """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                        UPDATE brand
                        SET profile_img_key = %s
                        WHERE brand_id = %s
                        '''

                    values = (key, brand_id)
                    await cursor.execute(query, values)
                    await connection.commit()
                    return {'status': 'ok'}
            except Exception as e:
                await connection.rollback()
                print(e)
                return {'status': 'failed', 'code': 'ed37ed2d', 'error': e.args[0]}



    @staticmethod
    async def delete_profile_img_key(brand_id: str) -> dict[str, str|int]:
        """Delete brand profile image key
        
        Args:
            - brand_id
                Unique brand ID

        Returns:
            - on success
                {"status": "ok"}
            - on error
                {'status': 'failed', 'code': 'ed37ed2d', 'error': `sql error status`}
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                        UPDATE brand
                        SET profile_img_key = %s
                        WHERE brand_id = %s
                        '''

                    values = (None, brand_id)
                    await cursor.execute(query, values)
                    await connection.commit()
                    return {'status': 'ok'}
            except Exception as e:
                await connection.rollback()
                print(e)
                return {'status': 'failed', 'code': 'ed37ed2d', 'error': e.args[0]}



class Fetch:
    @staticmethod
    async def brand_profile(brand_id: str) -> dict[str, str]:
        """
        Returns brand profile which includes `profile_img_key`, `entity_name` and `brand_name`
        if data exists or `None`
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''SELECT profile_img_key, entity_name, brand_name
                        FROM brand WHERE brand_id = %s
                        '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    brand_profile = await cursor.fetchone()
                    await connection.commit()
                    return brand_profile
            except Exception as e:
                await connection.rollback()
                print(e)
                return {'status': 'failed', 'error': e.args[0], 'code': 'cb02df09'}