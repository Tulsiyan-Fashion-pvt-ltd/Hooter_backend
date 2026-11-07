from quart import current_app, session
from asyncmy.cursors import DictCursor
from utils.encryption import TokenEncryption
from typing import Literal
from traceback import print_exc


class Write:
    @staticmethod
    async def add_store(brand_id: str, shopify_shop_name: str, shopify_refresh_token: str, refresh_token_expires_in) -> dict:
        """Add a new Shopify store for a user."""

        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Encrypt token before storage
                    encrypted_refresh_token = TokenEncryption.encrypt_token(shopify_refresh_token)

                    await cursor.execute('''
                        INSERT INTO shopify_stores (brand_id, shopify_shop_name, encrypted_shopify_refresh_token, refresh_token_expires_in_seconds)
                        VALUES (%s, %s, %s, %s)
                    ''', (brand_id, shopify_shop_name, encrypted_refresh_token, refresh_token_expires_in))

                    await conn.commit()

                    # Fetch and return the created store
                    store_id = cursor.lastrowid
                    return {'status': 'successful', 'message': 'Store added successfully', 'store_id': store_id, 'shop_name': shopify_shop_name}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error adding store: {str(e)}')

                    if hasattr(e, 'args') and e.args[0] == 1062:
                        return {'status': 'failed', 'error': 'Store already exists for this Shopify shop'}

                    return {'status': 'failed', 'error': f'Unable to add store: {str(e)}'}
                

    @staticmethod
    async def update_refresh_token(store_id: int, refresh_token: str, refresh_token_expires_in: int|None = None) -> dict[Literal["status"]| Literal["message"], str| int]:
        """Update shopify store refresh token."""

        pool = current_app.pool
        async with pool.acquire() as conn:
            try:
                encrypted_shopify_refresh_token = TokenEncryption.encrypt_token(refresh_token)
                async with conn.cursor(cursor=DictCursor) as cursor:
                    query = f'''
                    UPDATE shopify_stores
                    SET
                    encrypted_shopify_refresh_token=%s
                    {"refresh_token_expires_in_seconds=%s" if refresh_token_expires_in else ""}
                    WHERE store_id=%s
                    '''
                    values = (encrypted_shopify_refresh_token, refresh_token_expires_in, store_id) if refresh_token_expires_in else (encrypted_shopify_refresh_token, store_id) 

                    await cursor.execute(query, values)
                    await conn.commit()
                    return {'status': 'successul'}
            except Exception as e:
                await conn.rollback()
                print(f'Error updating store: {str(e)}')
                return {'status': 'error', 'message': f'Unable to update store: {str(e)}'}


    @staticmethod
    async def delete_store(store_id: int) -> dict:
        """Delete a store (soft delete - mark as inactive)."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute('''
                        UPDATE stores SET status = 'inactive' 
                        WHERE store_id = %s
                    ''', (store_id,))

                    affected_rows = cursor.rowcount
                    await conn.commit()

                    if affected_rows == 0:
                        return {'status': 'error', 'message': 'Store not found or unauthorized'}

                    return {'status': 'ok', 'message': 'Store deleted successfully'}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error deleting store: {str(e)}')
                    return {'status': 'error', 'message': f'Unable to delete store: {str(e)}'}



class Fetch:
    @staticmethod
    async def stores(brand_id: str) -> list:
        """Fetch all stores assigned to a brand."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute('''
                        SELECT s.store_id, s.shopify_shop_name
                        FROM shopify_stores s
                        WHERE s.brand_id = %s and status='active'
                        ORDER BY s.created_at DESC
                    ''', (brand_id,))

                    stores = await cursor.fetchall()
                    await conn.commit()
                    return {"stores": stores} if stores else {"stores": []}
                except Exception as e:
                    await conn.rollback()
                    print(f'Error fetching brand stores: {str(e)}')
                    return {'status': 'failed', 'error': e.args[0]}


    @staticmethod
    async def store_creds(store_id: str) -> dict[Literal["error"]| str, int| str]:
        """Get store ID for the brand
        
        Args:
            store_creds:
                Unique ID for shopify store
                
        Returns:
            - on success: store_id| None
            - on failure: dict[`Literal["error"]`, int] 
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                        SELECT shopify_shop_name, encrypted_shopify_refresh_token, created_at, updated_at, refresh_token_expires_in_seconds
                        FROM shopify_stores
                        WHERE store_id = %s
                        '''
                    values = (store_id, )
                    await cursor.execute(query, values)
                    creds = await cursor.fetchone()
                    await connection.commit()
                    if not creds:
                        return {'error': 'Could not fetch the refresh_token'}

                    encrypted_refresh_token = creds.pop('encrypted_shopify_refresh_token')
                    creds = creds["refresh_token"] = TokenEncryption.decrypt_token(encrypted_refresh_token) if encrypted_refresh_token else None
                    return creds
            except Exception as e:
                await connection.rollback()
                print("Could not fetch the encrypted_shopify_refresh_token")
                print(e)
                print_exc()
                return {"error", e.args[0]}



    @staticmethod
    async def shope_name(store_id: str) -> str| dict[Literal["error"]| str, int| str]:
        """Fetch the store name for a brand.
        
        Args:
            brand_id: Unique Id for brand

        Returns:
            On success: store_name `str`
            On failure: `dict[Literal["error"]: int| str]`
        """
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                store = None
                try:
                    await cursor.execute('''
                        SELECT shopify_shop_name
                        FROM shopify_stores
                        WHERE store_id = %s
                    ''', (store_id,))

                    store = await cursor.fetchone()
                    await conn.commit()
                    return store.get('shopify_shop_name') if store else None
                except Exception as e:
                    await conn.rollback()
                    print(f'Error fetching primary store: {str(e)}')
                return store