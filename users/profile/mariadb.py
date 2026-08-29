from quart import current_app
from asyncmy.cursors import DictCursor

class Fetch:
    @staticmethod
    async def user_details(userid):
        if userid is None:
            return ()
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cursor:
                try:
                    await cursor.execute(
                        '''
                        SELECT user_name,
                               phone_number,
                               user_email,
                               user_designation
                        FROM user_creds
                        WHERE user_id=%s
                        ''',
                        (userid,)
                    )
                    return await cursor.fetchone()
                except Exception as e:
                    print(f'encountered error while fetching user credentials\n{e}')
                    return None