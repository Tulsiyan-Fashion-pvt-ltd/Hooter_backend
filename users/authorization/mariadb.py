from quart import current_app
from asyncmy.cursors import DictCursor

class Fetch:
    @staticmethod
    async def user_access(user_id: str):
            pool = current_app.pool
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cursor:
                    try:
                        await cursor.execute(
                            '''
                            SELECT user_access
                            FROM user_creds
                            WHERE user_id=%s
                            ''',
                            (user_id,)
                        )
                        result = await cursor.fetchone()
                        return result["user_access"] if result else None
                    except Exception as e:
                        print(f'error encountered while fetching the user access\n{e}')
                        return None