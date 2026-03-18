from quart import current_app
from asyncmy.cursors import DictCursor
from utils.encryption import TokenEncryption


class Write:
    @staticmethod
    async def add_store(user_id: str, shopify_shop_name: str, shopify_access_token: str, store_name: str = None, is_primary: bool = False) -> dict:
        """Add a new Shopify store for a user."""

        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Encrypt token before storage
                    encrypted_token = TokenEncryption.encrypt_token(shopify_access_token)

                    # If this is the first store, make it primary
                    await cursor.execute('SELECT COUNT(*) FROM stores WHERE user_id = %s', (user_id,))
                    store_count_result = await cursor.fetchone()
                    store_count = list(store_count_result.values())[0] if store_count_result else 0
                    if store_count == 0:
                        is_primary = True

                    # If making this primary, unset other primary stores
                    if is_primary:
                        await cursor.execute('''
                            UPDATE stores SET is_primary = FALSE 
                            WHERE user_id = %s AND is_primary = TRUE
                        ''', (user_id,))

                    await cursor.execute('''
                        INSERT INTO stores (user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active)
                        VALUES (%s, %s, %s, %s, %s, TRUE)
                    ''', (user_id, shopify_shop_name, encrypted_token, store_name or shopify_shop_name, is_primary))

                    await conn.commit()

                    # Fetch and return the created store
                    store_id = cursor.lastrowid
                    store = await Fetch.get_store_by_id(store_id, user_id)
                    return {'status': 'ok', 'message': 'Store added successfully', 'store': store}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error adding store: {str(e)}')

                    if hasattr(e, 'args') and e.args[0] == 1062:
                        return {'status': 'error', 'message': 'Store already exists for this Shopify shop'}

                    return {'status': 'error', 'message': f'Unable to add store: {str(e)}'}

    @staticmethod
    async def update_store(store_id: int, user_id: str, **kwargs) -> dict:
        """Update store details."""

        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Verify ownership
                    await cursor.execute('SELECT user_id FROM stores WHERE store_id = %s', (store_id,))
                    result = await cursor.fetchone()
                    if not result or result['user_id'] != user_id:
                        return {'status': 'error', 'message': 'Unauthorized access'}

                    # Build update query
                    updates = []
                    params = []
                    allowed_fields = ['shopify_shop_name', 'shopify_access_token', 'store_name', 'is_primary']

                    for key, value in kwargs.items():
                        if key in allowed_fields:
                            if key == 'shopify_access_token':
                                encrypted_token = TokenEncryption.encrypt_token(value)
                                updates.append('shopify_access_token_encrypted = %s')
                                params.append(encrypted_token)
                            else:
                                updates.append(f'{key} = %s')
                                params.append(value)

                    if not updates:
                        return {'status': 'error', 'message': 'No valid fields to update'}

                    params.append(store_id)
                    params.append(user_id)

                    # If setting as primary, unset others
                    if 'is_primary' in kwargs and kwargs['is_primary']:
                        await cursor.execute('''
                            UPDATE stores SET is_primary = FALSE 
                            WHERE user_id = %s AND is_primary = TRUE AND store_id != %s
                        ''', (user_id, store_id))

                    query = f"UPDATE stores SET {', '.join(updates)} WHERE store_id = %s AND user_id = %s"
                    await cursor.execute(query, params)
                    await conn.commit()

                    store = await Fetch.get_store_by_id(store_id, user_id)
                    return {'status': 'ok', 'message': 'Store updated successfully', 'store': store}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error updating store: {str(e)}')
                    return {'status': 'error', 'message': f'Unable to update store: {str(e)}'}

    @staticmethod
    async def delete_store(store_id: int, user_id: str) -> dict:
        """Delete a store (soft delete - mark as inactive)."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute('''
                        UPDATE stores SET is_active = FALSE 
                        WHERE store_id = %s AND user_id = %s
                    ''', (store_id, user_id))

                    affected_rows = cursor.rowcount
                    await conn.commit()

                    if affected_rows == 0:
                        return {'status': 'error', 'message': 'Store not found or unauthorized'}

                    return {'status': 'ok', 'message': 'Store deleted successfully'}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error deleting store: {str(e)}')
                    return {'status': 'error', 'message': f'Unable to delete store: {str(e)}'}

    @staticmethod
    async def create_brand(brand_name: str, user_id: str, brand_logo: str = None, brand_description: str = None) -> dict:
        """Create a new brand and assign it to the user."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute('''
                        INSERT INTO brand (brand_name, brand_logo, brand_description)
                        VALUES (%s, %s, %s)
                    ''', (brand_name, brand_logo, brand_description))

                    brand_id = cursor.lastrowid

                    await cursor.execute('''
                        INSERT INTO brand_access (brand_id, user_id, permission_level)
                        VALUES (%s, %s, %s)
                    ''', (brand_id, user_id, 'owner'))

                    await conn.commit()

                    brand = await Fetch.get_brand_by_id(brand_id)
                    return {'status': 'ok', 'message': 'Brand created successfully', 'brand': brand}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error creating brand: {str(e)}')

                    if hasattr(e, 'args') and e.args[0] == 1062:
                        return {'status': 'error', 'message': 'Brand already exists'}

                    return {'status': 'error', 'message': f'Unable to create brand: {str(e)}'}

    @staticmethod
    async def create_product(uid: str, brand_id: int, title: str, description: str, **kwargs) -> dict:
        """Create a new product (fashion entry) with extended attributes."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    # Create uid record first (associate with brand)
                    await cursor.execute('INSERT INTO uid_record (uid, brand_id) VALUES (%s, %s)', (uid, brand_id))

                    # Insert fashion product
                    await cursor.execute('''
                        INSERT INTO fashion (
                            uid, brand_id, title, description, vendor, product_type, tags,
                            status, price, compare_at_price, sku, barcode, weight, weight_unit,
                            collections, brand_color, product_remark, series_length_ankle,
                            series_rise_waist, series_knee, gender, fit_type, print_type,
                            material, material_composition, care_instruction, art_technique, stitch_type
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    ''', (
                        uid, brand_id, title, description,
                        kwargs.get('vendor'), kwargs.get('product_type'), kwargs.get('tags'),
                        kwargs.get('status', 'ACTIVE'),
                        kwargs.get('price'), kwargs.get('compare_at_price'),
                        kwargs.get('sku'), kwargs.get('barcode'),
                        kwargs.get('weight'), kwargs.get('weight_unit'),
                        kwargs.get('collections'), kwargs.get('brand_color'),
                        kwargs.get('product_remark'), kwargs.get('series_length_ankle'),
                        kwargs.get('series_rise_waist'), kwargs.get('series_knee'),
                        kwargs.get('gender'), kwargs.get('fit_type'), kwargs.get('print_type'),
                        kwargs.get('material'), kwargs.get('material_composition'),
                        kwargs.get('care_instruction'), kwargs.get('art_technique'), kwargs.get('stitch_type')
                    ))

                    await conn.commit()
                    return {'status': 'ok', 'message': 'Product created successfully', 'uid': uid}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error creating product: {str(e)}')

                    if hasattr(e, 'args') and e.args[0] == 1062:
                        return {'status': 'error', 'message': 'Product UID already exists'}

                    return {'status': 'error', 'message': f'Unable to create product: {str(e)}'}

    @staticmethod
    async def update_product(uid: str, brand_id: int, **kwargs) -> dict:
        """Update product details."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    updates = []
                    params = []
                    allowed_fields = [
                        'title', 'description', 'vendor', 'product_type', 'tags', 'status',
                        'price', 'compare_at_price', 'sku', 'barcode', 'weight', 'weight_unit',
                        'collections', 'brand_color', 'product_remark', 'series_length_ankle',
                        'series_rise_waist', 'series_knee', 'gender', 'fit_type', 'print_type',
                        'material', 'material_composition', 'care_instruction', 'art_technique', 'stitch_type'
                    ]

                    for key, value in kwargs.items():
                        if key in allowed_fields:
                            updates.append(f'{key} = %s')
                            params.append(value)

                    if not updates:
                        return {'status': 'error', 'message': 'No valid fields to update'}

                    params.extend([uid, brand_id])
                    query = f"UPDATE fashion SET {', '.join(updates)} WHERE uid = %s AND brand_id = %s"
                    await cursor.execute(query, params)
                    await conn.commit()

                    return {'status': 'ok', 'message': 'Product updated successfully', 'uid': uid}

                except Exception as e:
                    await conn.rollback()
                    print(f'Error updating product: {str(e)}')
                    return {'status': 'error', 'message': f'Unable to update product: {str(e)}'}


class Fetch:
    @staticmethod
    async def get_user_stores(user_id: str) -> list:
        """Fetch all stores for a user."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                stores = []
                try:
                    await cursor.execute('''
                        SELECT store_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active
                        FROM stores
                        WHERE user_id = %s AND is_active = TRUE
                        ORDER BY is_primary DESC, created_at DESC
                    ''', (user_id,))

                    stores = await cursor.fetchall()
                except Exception as e:
                    print(f'Error fetching user stores: {str(e)}')
                return stores

    @staticmethod
    async def get_store_by_id(store_id: int, user_id: str = None) -> dict:
        """Fetch a specific store. Optionally verify user ownership."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                store = None
                try:
                    if user_id:
                        await cursor.execute('''
                            SELECT store_id, user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active
                            FROM stores
                            WHERE store_id = %s AND user_id = %s
                        ''', (store_id, user_id))
                    else:
                        await cursor.execute('''
                            SELECT store_id, user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active
                            FROM stores
                            WHERE store_id = %s
                        ''', (store_id,))

                    store = await cursor.fetchone()
                except Exception as e:
                    print(f'Error fetching store: {str(e)}')
                return store

    @staticmethod
    async def get_primary_store(user_id: str) -> dict:
        """Fetch the primary store for a user."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                store = None
                try:
                    await cursor.execute('''
                        SELECT store_id, user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary
                        FROM stores
                        WHERE user_id = %s AND is_primary = TRUE AND is_active = TRUE
                        LIMIT 1
                    ''', (user_id,))

                    store = await cursor.fetchone()
                except Exception as e:
                    print(f'Error fetching primary store: {str(e)}')
                return store

    @staticmethod
    async def get_user_brands(user_id: str) -> list:
        """Fetch all brands assigned to a user."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                brands = []
                try:
                    await cursor.execute('''
                        SELECT b.brand_id, b.brand_name, b.brand_logo, b.brand_description, b.created_at
                        FROM brand b
                        INNER JOIN brand_access ba ON b.brand_id = ba.brand_id
                        WHERE ba.user_id = %s
                        ORDER BY b.created_at DESC
                    ''', (user_id,))

                    brands = await cursor.fetchall()
                except Exception as e:
                    print(f'Error fetching user brands: {str(e)}')
                return brands

    @staticmethod
    async def get_brand_by_id(brand_id: int) -> dict:
        """Fetch a specific brand by ID."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                brand = None
                try:
                    await cursor.execute('''
                        SELECT brand_id, brand_name, brand_logo, brand_description, created_at
                        FROM brand
                        WHERE brand_id = %s
                    ''', (brand_id,))

                    brand = await cursor.fetchone()
                except Exception as e:
                    print(f'Error fetching brand: {str(e)}')
                return brand

    @staticmethod
    async def verify_brand_ownership(brand_id: int, user_id: str) -> bool:
        """Verify that a user has access to a brand."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute(
                        'SELECT brand_id FROM brand_access WHERE brand_id = %s AND user_id = %s',
                        (brand_id, user_id)
                    )
                    result = await cursor.fetchone()
                    return result is not None
                except Exception as e:
                    print(f'Error verifying brand ownership: {str(e)}')
                    return False

    @staticmethod
    async def get_product_by_uid(uid: str, brand_id: int = None) -> dict:
        """Retrieve product details by uid with optional brand verification."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                product = None
                try:
                    if brand_id:
                        await cursor.execute('''
                            SELECT uid, brand_id, title, description, vendor, product_type, tags,
                                   status, price, compare_at_price, sku, barcode, weight, weight_unit,
                                   collections, brand_color, product_remark, series_length_ankle,
                                   series_rise_waist, series_knee, gender, fit_type, print_type,
                                   material, material_composition, care_instruction, art_technique,
                                   stitch_type, created_at, updated_at
                            FROM fashion
                            WHERE uid = %s AND brand_id = %s
                        ''', (uid, brand_id))
                    else:
                        await cursor.execute('''
                            SELECT uid, brand_id, title, description, vendor, product_type, tags,
                                   status, price, compare_at_price, sku, barcode, weight, weight_unit,
                                   collections, brand_color, product_remark, series_length_ankle,
                                   series_rise_waist, series_knee, gender, fit_type, print_type,
                                   material, material_composition, care_instruction, art_technique,
                                   stitch_type, created_at, updated_at
                            FROM fashion
                            WHERE uid = %s
                        ''', (uid,))

                    product = await cursor.fetchone()
                except Exception as e:
                    print(f'Error fetching product: {str(e)}')
                return product

    @staticmethod
    async def list_products(brand_id: int, limit: int = 50, offset: int = 0, status: str = None, search: str = None) -> list:
        """List products for a brand with optional filtering."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    where_clauses = ["f.brand_id = %s"]
                    params = [brand_id]

                    if status:
                        where_clauses.append("f.status = %s")
                        params.append(status.upper())

                    if search:
                        where_clauses.append("(f.title LIKE %s OR f.vendor LIKE %s OR f.sku LIKE %s)")
                        search_term = f"%{search}%"
                        params.extend([search_term, search_term, search_term])

                    query = f"""
                        SELECT f.uid, f.brand_id, f.title, f.price, f.vendor, f.status,
                               f.created_at, COUNT(DISTINCT li.id) as image_count
                        FROM fashion f
                        LEFT JOIN low_resol_images li ON f.uid = li.uid
                        WHERE {' AND '.join(where_clauses)}
                        GROUP BY f.uid
                        ORDER BY f.created_at DESC
                        LIMIT %s OFFSET %s
                    """
                    params.extend([limit, offset])
                    await cursor.execute(query, tuple(params))

                    results = await cursor.fetchall()
                    return [
                        {
                            'uid': row['uid'],
                            'brand_id': row['brand_id'],
                            'title': row['title'],
                            'price': row['price'],
                            'vendor': row['vendor'],
                            'status': row['status'],
                            'created_at': row['created_at'],
                            'images_count': row['image_count']
                        }
                        for row in results
                    ]

                except Exception as e:
                    print(f'Error listing products: {str(e)}')
                    return []



