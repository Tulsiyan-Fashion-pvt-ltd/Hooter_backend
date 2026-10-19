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

    # @staticmethod
    # async def create_brand(brand_name: str, user_id: str, brand_logo: str = None, brand_description: str = None) -> dict:
    #     """Create a new brand and assign it to the user."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             try:
    #                 await cursor.execute('''
    #                     INSERT INTO brand (brand_name, brand_logo, brand_description)
    #                     VALUES (%s, %s, %s)
    #                 ''', (brand_name, brand_logo, brand_description))

    #                 brand_id = cursor.lastrowid

    #                 await cursor.execute('''
    #                     INSERT INTO brand_access (brand_id, user_id, permission_level)
    #                     VALUES (%s, %s, %s)
    #                 ''', (brand_id, user_id, 'owner'))

    #                 await conn.commit()

    #                 brand = await Fetch.get_brand_by_id(brand_id)
    #                 return {'status': 'ok', 'message': 'Brand created successfully', 'brand': brand}

    #             except Exception as e:
    #                 await conn.rollback()
    #                 print(f'Error creating brand: {str(e)}')

    #                 if hasattr(e, 'args') and e.args[0] == 1062:
    #                     return {'status': 'error', 'message': 'Brand already exists'}

    #                 return {'status': 'error', 'message': f'Unable to create brand: {str(e)}'}

    # @staticmethod
    # async def create_product(uid: str, brand_id: int, title: str, description: str, **kwargs) -> dict:
    #     """Create a new product (fashion entry) with extended attributes."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             try:
    #                 # Create uid record first (associate with brand)
    #                 await cursor.execute('INSERT INTO uid_record (uid, brand_id) VALUES (%s, %s)', (uid, brand_id))

    #                 # Insert fashion product
    #                 await cursor.execute('''
    #                     INSERT INTO fashion (
    #                         uid, brand_id, title, description, vendor, product_type, tags,
    #                         status, price, compare_at_price, sku, barcode, weight, weight_unit,
    #                         collections, brand_color, product_remark, series_length_ankle,
    #                         series_rise_waist, series_knee, gender, fit_type, print_type,
    #                         material, material_composition, care_instruction, art_technique, stitch_type
    #                     ) VALUES (
    #                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    #                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    #                     )
    #                 ''', (
    #                     uid, brand_id, title, description,
    #                     kwargs.get('vendor'), kwargs.get('product_type'), kwargs.get('tags'),
    #                     kwargs.get('status', 'ACTIVE'),
    #                     kwargs.get('price'), kwargs.get('compare_at_price'),
    #                     kwargs.get('sku'), kwargs.get('barcode'),
    #                     kwargs.get('weight'), kwargs.get('weight_unit'),
    #                     kwargs.get('collections'), kwargs.get('brand_color'),
    #                     kwargs.get('product_remark'), kwargs.get('series_length_ankle'),
    #                     kwargs.get('series_rise_waist'), kwargs.get('series_knee'),
    #                     kwargs.get('gender'), kwargs.get('fit_type'), kwargs.get('print_type'),
    #                     kwargs.get('material'), kwargs.get('material_composition'),
    #                     kwargs.get('care_instruction'), kwargs.get('art_technique'), kwargs.get('stitch_type')
    #                 ))

    #                 await conn.commit()
    #                 return {'status': 'ok', 'message': 'Product created successfully', 'uid': uid}

    #             except Exception as e:
    #                 await conn.rollback()
    #                 print(f'Error creating product: {str(e)}')

    #                 if hasattr(e, 'args') and e.args[0] == 1062:
    #                     return {'status': 'error', 'message': 'Product UID already exists'}

    #                 return {'status': 'error', 'message': f'Unable to create product: {str(e)}'}

    # @staticmethod
    # async def update_product(uid: str, brand_id: int, **kwargs) -> dict:
    #     """Update product details."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             try:
    #                 updates = []
    #                 params = []
    #                 allowed_fields = [
    #                     'title', 'description', 'vendor', 'product_type', 'tags', 'status',
    #                     'price', 'compare_at_price', 'sku', 'barcode', 'weight', 'weight_unit',
    #                     'collections', 'brand_color', 'product_remark', 'series_length_ankle',
    #                     'series_rise_waist', 'series_knee', 'gender', 'fit_type', 'print_type',
    #                     'material', 'material_composition', 'care_instruction', 'art_technique', 'stitch_type'
    #                 ]

    #                 for key, value in kwargs.items():
    #                     if key in allowed_fields:
    #                         updates.append(f'{key} = %s')
    #                         params.append(value)

    #                 if not updates:
    #                     return {'status': 'error', 'message': 'No valid fields to update'}

    #                 params.extend([uid, brand_id])
    #                 query = f"UPDATE fashion SET {', '.join(updates)} WHERE uid = %s AND brand_id = %s"
    #                 await cursor.execute(query, params)
    #                 await conn.commit()

    #                 return {'status': 'ok', 'message': 'Product updated successfully', 'uid': uid}

    #             except Exception as e:
    #                 await conn.rollback()
    #                 print(f'Error updating product: {str(e)}')
    #                 return {'status': 'error', 'message': f'Unable to update product: {str(e)}'}


class Fetch:
    @staticmethod
    async def get_brand_stores(brand_id: str) -> list:
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
                    return {"stores": stores} if stores else {"stores": []}
                except Exception as e:
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

                    if not creds:
                        return {'error': 'Could not fetch the refresh_token'}

                    encrypted_refresh_token = creds.pop('encrypted_shopify_refresh_token')
                    creds = creds["refresh_token"] = TokenEncryption.decrypt_token(encrypted_refresh_token) if encrypted_refresh_token else None
                    return creds
            except Exception as e:
                print("Could not fetch the encrypted_shopify_refresh_token")
                print(e)
                print_exc()
                return {"error", e.args[0]}



    @staticmethod
    async def shope_name(store_id: str) -> str| dict[Literal["error"], int| str]:
        """Fetch the store name for a brand.
        
        Args:
            brand_id: Unique Id for brand

        Returns:
            On success: store_name 
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
                except Exception as e:
                    print(f'Error fetching primary store: {str(e)}')
                return store



    # @staticmethod
    # async def get_store_by_id(store_id: int) -> dict:
    #     """Fetch a specific store. Optionally verify user ownership.
    #         Returns: shopify_shop_name, shopify_access_token_encrypted
    #     """
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             try:
    #                 await cursor.execute('''
    #                     SELECT shopify_shop_name, status, created_at, updated_at
    #                     FROM stores
    #                     WHERE store_id = %s
    #                 ''', (store_id, ))

    #                 store = await cursor.fetchone()
    #                 return store
    #             except Exception as e:
    #                 print(f'Error fetching store: {str(e)}')
    #                 return {'status': 'failed', 'error': e.args[0]}


    # @staticmethod
    # async def store_id(brand_id: str) -> str| None| dict[Literal["error"], int]:
    #     """Get store ID for the brand
        
    #     Args:
    #         brand_id:
    #             Unique ID for brands
                
    #     Returns:
    #         - on success: store_id| None
    #         - on failure: dict[`Literal["error"]`, int] 
    #     """
    #     pool = current_app.pool
    #     async with pool.acquire() as connection:
    #         try:
    #             async with connection.cursor(cursor=DictCursor) as cursor:
    #                 query = '''
    #                     SELECT store_id FROM shopify_stores
    #                     WHERE brand_id = %s
    #                     '''
    #                 values = (brand_id, )
    #                 await cursor.execute(query, values)
    #                 store_id = await cursor.fetchone()
    #                 store_id = store_id.get('store_id') if store_id else None
    #         except Exception as e:
    #             print("Could not fetch the store_id")
    #             print(e)
    #             print_exc()
    #             return {"error", e.args[0]}





    # @staticmethod
    # async def get_brand_by_id(brand_id: int) -> dict:
    #     """Fetch a specific brand by ID."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             brand = None
    #             try:
    #                 await cursor.execute('''
    #                     SELECT brand_id, brand_name, brand_logo, brand_description, created_at
    #                     FROM brand
    #                     WHERE brand_id = %s
    #                 ''', (brand_id,))

    #                 brand = await cursor.fetchone()
    #             except Exception as e:
    #                 print(f'Error fetching brand: {str(e)}')
    #             return brand

    # @staticmethod
    # async def verify_brand_ownership(brand_id: int, user_id: str) -> bool:
    #     """Verify that a user has access to a brand."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             try:
    #                 await cursor.execute(
    #                     'SELECT brand_id FROM brand_access WHERE brand_id = %s AND user_id = %s',
    #                     (brand_id, user_id)
    #                 )
    #                 result = await cursor.fetchone()
    #                 return result is not None
    #             except Exception as e:
    #                 print(f'Error verifying brand ownership: {str(e)}')
    #                 return False

    # @staticmethod
    # async def get_product_by_uid(uid: str, brand_id: int = None) -> dict:
    #     """Retrieve product details by uid with optional brand verification."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             product = None
    #             try:
    #                 if brand_id:
    #                     await cursor.execute('''
    #                         SELECT uid, brand_id, title, description, vendor, product_type, tags,
    #                                status, price, compare_at_price, sku, barcode, weight, weight_unit,
    #                                collections, brand_color, product_remark, series_length_ankle,
    #                                series_rise_waist, series_knee, gender, fit_type, print_type,
    #                                material, material_composition, care_instruction, art_technique,
    #                                stitch_type, created_at, updated_at
    #                         FROM fashion
    #                         WHERE uid = %s AND brand_id = %s
    #                     ''', (uid, brand_id))
    #                 else:
    #                     await cursor.execute('''
    #                         SELECT uid, brand_id, title, description, vendor, product_type, tags,
    #                                status, price, compare_at_price, sku, barcode, weight, weight_unit,
    #                                collections, brand_color, product_remark, series_length_ankle,
    #                                series_rise_waist, series_knee, gender, fit_type, print_type,
    #                                material, material_composition, care_instruction, art_technique,
    #                                stitch_type, created_at, updated_at
    #                         FROM fashion
    #                         WHERE uid = %s
    #                     ''', (uid,))

    #                 product = await cursor.fetchone()
    #             except Exception as e:
    #                 print(f'Error fetching product: {str(e)}')
    #             return product

    # @staticmethod
    # async def list_products(brand_id: int, limit: int = 50, offset: int = 0, status: str = None, search: str = None) -> list:
    #     """List products for a brand with optional filtering."""
    #     pool = current_app.pool
    #     async with pool.acquire() as conn:
    #         async with conn.cursor(cursor=DictCursor) as cursor:
    #             try:
    #                 where_clauses = ["f.brand_id = %s"]
    #                 params = [brand_id]

    #                 if status:
    #                     where_clauses.append("f.status = %s")
    #                     params.append(status.upper())

    #                 if search:
    #                     where_clauses.append("(f.title LIKE %s OR f.vendor LIKE %s OR f.sku LIKE %s)")
    #                     search_term = f"%{search}%"
    #                     params.extend([search_term, search_term, search_term])

    #                 query = f"""
    #                     SELECT f.uid, f.brand_id, f.title, f.price, f.vendor, f.status,
    #                            f.created_at, COUNT(DISTINCT li.id) as image_count
    #                     FROM fashion f
    #                     LEFT JOIN low_resol_images li ON f.uid = li.uid
    #                     WHERE {' AND '.join(where_clauses)}
    #                     GROUP BY f.uid
    #                     ORDER BY f.created_at DESC
    #                     LIMIT %s OFFSET %s
    #                 """
    #                 params.extend([limit, offset])
    #                 await cursor.execute(query, tuple(params))

    #                 results = await cursor.fetchall()
    #                 return [
    #                     {
    #                         'uid': row['uid'],
    #                         'brand_id': row['brand_id'],
    #                         'title': row['title'],
    #                         'price': row['price'],
    #                         'vendor': row['vendor'],
    #                         'status': row['status'],
    #                         'created_at': row['created_at'],
    #                         'images_count': row['image_count']
    #                     }
    #                     for row in results
    #                 ]

    #             except Exception as e:
    #                 print(f'Error listing products: {str(e)}')
    #                 return []



