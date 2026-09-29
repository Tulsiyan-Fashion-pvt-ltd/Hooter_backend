from quart import current_app
from asyncmy.cursors import DictCursor
from utils.encryption import TokenEncryption


class Write:
    @staticmethod
    async def add_store(brand_id: str, selling_partner_id: str, refresh_token: str) -> dict:
        """Add an amazon seller connection."""
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Encrypt refresh token before storage
                    encrypted_token = TokenEncryption.encrypt_token(refresh_token)

                    await cursor.execute('''
                        INSERT INTO amazon_stores (brand_id, selling_partner_id, amazon_refresh_token_encrypted)
                        VALUES (%s, %s, %s)
                    ''', (brand_id, selling_partner_id, encrypted_token))

                    await conn.commit()
                    return {'status': 'ok', 'message': 'Amazon store added successfully', 'selling_partner_id': selling_partner_id}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error adding Amazon store: {str(e)}')

                    if hasattr(e, 'args') and e.args[0] == 1062:
                        return {'status': 'error', 'message': 'Amazon seller is already connected'}

                    return {'status': 'error', 'message': f'Unable to add Amazon store: {str(e)}'}



class Fetch:

    @staticmethod
    async def get_store(brand_id: str, selling_partner_id: str) -> dict:
        """Fetch an Amazon seller connection."""
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute('''
                        SELECT store_id, brand_id, selling_partner_id, amazon_refresh_token_encrypted
                        FROM amazon_stores
                        WHERE brand_id = %s AND selling_partner_id = %s
                    ''', (brand_id, selling_partner_id))
                    return await cursor.fetchone()

                except Exception as e:
                    print(f'Error fetching Amazon store: {str(e)}')
                    return None