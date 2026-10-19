from quart import current_app
from asyncmy.cursors import DictCursor
from utils.encryption import TokenEncryption
import time


class Write:
    @staticmethod
    async def add_account(brand_id: str,access_token: str,refresh_token: str,expires_in: int,refresh_token_expires_in: int = None,scope: str = None,) -> dict:
        """Add a Flipkart account connection."""
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Encrypt tokens before storage.
                    encrypted_access_token = (TokenEncryption.encrypt_token(access_token))
                    encrypted_refresh_token = (TokenEncryption.encrypt_token(refresh_token))
                    access_token_expires_at = (int(time.time()) + int(expires_in))

                    refresh_token_expires_at = (
                        int(time.time()) + int(refresh_token_expires_in)
                        if refresh_token_expires_in is not None
                        else None
                    )

                    await cursor.execute(
                        """
                        INSERT INTO flipkart_accounts (brand_id,access_token_encrypted,refresh_token_encrypted,access_token_expires_at,refresh_token_expires_at,scope)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,(brand_id, encrypted_access_token, encrypted_refresh_token, access_token_expires_at, refresh_token_expires_at, scope,),
                    )

                    await conn.commit()
                    return {"status": "ok", "message": "Flipkart account added successfully", "brand_id": brand_id,}

                except Exception as e:
                    await conn.rollback()
                    print(f"Error adding Flipkart account: {str(e)}")

                    if hasattr(e, "args") and e.args[0] == 1062:
                        return {"status": "error", "message": "A Flipkart account is already connected",}

                    return { "status": "error", "message": (f"Unable to add Flipkart account: {str(e)}" ),}

    @staticmethod
    async def update_tokens(brand_id: str, access_token: str, expires_in: int, refresh_token: str = None, refresh_token_expires_in: int = None,) -> dict:
        """Update tokens after refreshing a Flipkart access token."""
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    encrypted_access_token = (TokenEncryption.encrypt_token(access_token))
                    access_token_expires_at = (int(time.time()) + int(expires_in))

                    await cursor.execute(
                        """
                        UPDATE flipkart_accounts
                        SET access_token_encrypted = %s, access_token_expires_at = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE brand_id = %s
                        """,(encrypted_access_token, access_token_expires_at, brand_id,),
                    )

                    if cursor.rowcount == 0:
                        await conn.rollback()
                        return {"status": "error", "message": ("Flipkart account not found for this brand"),}

                    # Update refresh token only if Flipkart returned one.
                    if refresh_token:
                        encrypted_refresh_token = (TokenEncryption.encrypt_token(refresh_token))

                        if refresh_token_expires_in is not None:
                            refresh_token_expires_at = (int(time.time()) + int(refresh_token_expires_in))

                            await cursor.execute(
                                """
                                UPDATE flipkart_accounts
                                SET refresh_token_encrypted = %s, refresh_token_expires_at = %s
                                WHERE brand_id = %s
                                """,(encrypted_refresh_token, refresh_token_expires_at, brand_id,),
                            )
                        else:
                            await cursor.execute(
                                """
                                UPDATE flipkart_accounts
                                SET refresh_token_encrypted = %s
                                WHERE brand_id = %s
                                """, (encrypted_refresh_token, brand_id),
                            )

                    await conn.commit()
                    return {"status": "ok", "message": "Flipkart tokens updated successfully",}

                except Exception as e:
                    await conn.rollback()
                    print(f"Error updating Flipkart tokens: {str(e)}")

                    return {"status": "error", "message": (f"Unable to update Flipkart tokens: {str(e)}"),}


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
                        SELECT brand_id, access_token_encrypted, refresh_token_encrypted, access_token_expires_at, refresh_token_expires_at, scope
                        FROM flipkart_accounts
                        WHERE brand_id = %s
                        """, (brand_id,),
                    )

                    account = await cursor.fetchone()

                    if not account:
                        return None

                    # Decrypt tokens before returning them.
                    account["access_token"] = (
                        TokenEncryption.decrypt_token(account["access_token_encrypted"])
                    )
                    account["refresh_token"] = (
                        TokenEncryption.decrypt_token(account["refresh_token_encrypted"])
                    )

                    return account

                except Exception as e:
                    print(f"Error fetching Flipkart account: {str(e)}")
                    return None