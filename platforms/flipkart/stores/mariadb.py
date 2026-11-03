from quart import current_app
from asyncmy.cursors import DictCursor
from utils.encryption import TokenEncryption
import time


class Write:
    @staticmethod
    async def add_account(brand_id: str, refresh_token: str, refresh_token_expires_in: int,) -> dict:
        """Add a Flipkart seller connection."""
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Encrypt refresh token before storage
                    encrypted_token = TokenEncryption.encrypt_token(refresh_token)

                    await cursor.execute(
                        """
                        INSERT INTO flipkart_sellers (brand_id, refresh_token, refresh_token_expires_in)
                        VALUES (%s, %s, %s)
                        """, (brand_id, encrypted_token, refresh_token_expires_in,),
                    )

                    await conn.commit()

                    return {
                        "status": "ok",
                        "message": "Flipkart seller added successfully",
                        "seller_id": cursor.lastrowid
                    }

                except Exception as e:
                    await conn.rollback()
                    print(f"Error adding Flipkart seller: {str(e)}")

                    if hasattr(e, "args") and e.args[0] == 1062:
                        return {"status": "error", "message": "A Flipkart account is already connected"}

                    return {"status": "error", "message": f"Unable to add Flipkart seller: {str(e)}"}




    @staticmethod
    async def update_tokens(brand_id: str, refresh_token: str = None, refresh_token_expires_in: int = None,) -> dict:
        """Update the Flipkart refresh token."""
        pool = current_app.pool
    
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    if refresh_token:
                        encrypted_refresh_token = (TokenEncryption.encrypt_token(refresh_token))
    
                        if refresh_token_expires_in is not None:
                            await cursor.execute(
                                """
                                UPDATE flipkart_sellers
                                SET refresh_token = %s,
                                    refresh_token_expires_in = %s
                                WHERE brand_id = %s
                                """,
                                (encrypted_refresh_token, refresh_token_expires_in, brand_id,),
                            )
                        else:
                            await cursor.execute(
                                """
                                UPDATE flipkart_sellers
                                SET refresh_token = %s
                                WHERE brand_id = %s
                                """,
                                (encrypted_refresh_token, brand_id),
                            )
    
                        if cursor.rowcount == 0:
                            await conn.rollback()
                            return {"status": "error", "message": "Flipkart account not found for this brand"}
    
                    await conn.commit()
    
                    return {"status": "ok", "message": "Flipkart tokens updated successfully"}
    
                except Exception as e:
                    await conn.rollback()
                    print(f"Error updating Flipkart tokens: {str(e)}")
    
                    return {"status": "error", "message": "Unable to update Flipkart tokens"}


class Fetch:
    @staticmethod
    async def get_account(brand_id: str) -> dict:
        """Fetch a Flipkart account connection."""
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute(
                        """
                        SELECT seller_id, brand_id, refresh_token, refresh_token_expires_in, updated_at
                        FROM flipkart_sellers
                        WHERE brand_id = %s
                        """, (brand_id,),
                    )

                    account = await cursor.fetchone()

                    if not account:
                        return None

                    account["refresh_token"] = (
                        TokenEncryption.decrypt_token(account["refresh_token"])
                    )

                    return account

                except Exception as e:
                    print(f"Error fetching Flipkart account: {str(e)}")
                    return None