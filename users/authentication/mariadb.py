from quart import current_app
from datetime import datetime
from asyncmy.cursors import DictCursor

class Write:
    @staticmethod
    async def signup_user(user_creds):
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    userid = user_creds.get('userid')
                    hashed_password = user_creds.get('hashed_password')
                    name = user_creds.get('name')
                    number = user_creds.get('number')
                    email = user_creds.get('email')
                    designation = user_creds.get('designation')

                    await cursor.execute(
                        '''
                        INSERT INTO users(user_id, user_password)
                        VALUES(%s, %s)
                        ''',
                        (userid, hashed_password)
                    )

                    await cursor.execute(
                        '''
                        INSERT INTO user_creds(
                            user_id,
                            user_name,
                            phone_number,
                            user_email,
                            user_access,
                            user_designation,
                            created_at
                        )
                        VALUES(%s, %s, %s, %s, %s, %s, CURDATE())
                        ''',
                        (userid, name, number, email, 'super_user', designation)
                    )

                    await conn.commit()

                except Exception as e:
                    await conn.rollback()
                    print(f'Error encountered while signing up user: {e}')

                    if hasattr(e, "args") and e.args and e.args[0] == 1062:
                        return {
                            'status': 'error',
                            'message': 'user_already_registered'
                        }

                    return {
                        'status': 'error',
                        'message': 'unable_to_register_user'
                    }

        return {
            'status': 'ok',
            'message': 'user_registration_successful'
        }
class Fetch:
    @staticmethod
    async def userid_by_email(email):
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                userid = None
                try:          
                    await cursor.execute('''
                                   select user_id from user_creds where user_email=%s
                                   ''', (email, ))
                    userid = await cursor.fetchone()
                    userid = userid.get('user_id')
                    # userid = userid[0] if userid and len(userid) != 0 else None
                except Exception as e:
                    print(f"error while checking the checking the credentials for login as\n{e}")
                return userid
            
    @staticmethod
    async def user_password(userid: str) -> dict:
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(DictCursor) as cursor:
                try:
                    await cursor.execute(
                        '''
                        SELECT user_password
                        FROM users
                        WHERE user_id=%s
                        ''',
                        (userid)
                    )
                    result = await cursor.fetchone()
                    return result
                except Exception as e:
                    print(f'error occurred while checking the password as {e}')
                    return {}
                
                
    @staticmethod
    async def user_access(user_id: str) -> str:
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