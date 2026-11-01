import asyncmy
from asyncmy.cursors import DictCursor
import os
from dotenv import load_dotenv
from traceback import print_exc
import aiofiles
import json
from utils.encryption import TokenEncryption
from utils.lookup_hash import create_hash

load_dotenv()



pool = None

async def get_pool():
    global pool
    if pool == None:
        pool = await asyncmy.create_pool(
            host=os.environ.get('EXTERNAL_DB_HOST'),
            port=int(os.environ.get('EXTERNAL_DB_PORT')),
            user=os.environ.get('HOOTER_DB_USER'),
            password=os.environ.get('HOOTER_DB_PASSWORD'),
            db=os.environ.get('HOOTER_DB')
        )

    return pool


async def run():
    '''
    Encrypt and migrate the data
    '''
    pool = await get_pool()
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                query = '''SELECT user_id, user_name, phone_number, user_email, user_designation, created_at
                    FROM user_creds'''

                await cursor.execute(query)
                data = await cursor.fetchall()

                '''preparing backup in case'''
                # this is under dev that why it's a cheap migration
                async with aiofiles.open('.migration_backup.json', 'w') as backup_file:
                    await backup_file.write(
                        json.dumps(data, default=str, indent=2)
                    )


                query = '''DROP TABLE user_creds'''
                await cursor.execute(query)

                async with aiofiles.open('.migration_backup.json', 'r') as backup_file:
                    data = await backup_file.read()
                    data = json.loads(data)

                '''Create new formatted table'''
                query = '''CREATE TABLE `user_creds`
                    (
                        `user_id` CHAR(36) PRIMARY KEY,
                        `user_name` varchar(255) NOT NULL,
                        `phone_number` varchar(255) NOT NULL,
                        `phone_number_lookup` char(64) NOT NULL UNIQUE,
                        `user_email` varchar(512) NOT NULL,
                        `email_lookup` char(64) NOT NULL UNIQUE,
                        `user_designation` varchar(255) NOT NULL,
                        `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
                        `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
                    )
                    '''
                await cursor.execute(query)


                '''migrate encrypted data'''
                query = '''INSERT INTO user_creds (user_id, user_name, phone_number, phone_number_lookup, user_email, email_lookup,
                    user_designation, created_at)
                    values(%s, %s, %s, %s, %s, %s, %s, %s)'''

                
                for user in data:
                    values = (user.get('user_id'),
                              TokenEncryption.encrypt_token(user.get('user_name')),
                              TokenEncryption.encrypt_token(user.get('phone_number')),
                              create_hash(user.get('phone_number')),
                              TokenEncryption.encrypt_token(user.get('user_email')),
                              create_hash(user.get('user_email')),
                              user.get('user_designation'),
                              user.get('created_at')
                              )

                    await cursor.execute(query, values)

                await connection.commit()
        except Exception as e:
            await connection.rollback()
            print(e)
            print_exc()

