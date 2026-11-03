"THIS MODULE STORE THE TOKEN OR AUTHENTICAITON RELATED DB CALLS"

from quart import current_app
from asyncmy.cursors import DictCursor
from utils.encryption import TokenEncryption



class Fetch:
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