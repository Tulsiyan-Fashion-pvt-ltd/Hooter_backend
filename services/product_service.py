import json
import uuid
import time
import requests
from quart import current_app
from asyncmy.cursors import DictCursor
from sql_queries.storesdb import Fetch, Write
from services.shopify_graphql import ShopifyRetryableError
from services.shopify_helpers import get_store_config
from services.exceptions import AuthorizationError, ShopifyAPIError, ValidationError, IdempotencyConflict


class ProductService:
    """Service for managing products (brand-centric) with Shopify sync and strict isolation."""

    @staticmethod
    async def verify_brand_ownership(brand_id: int, user_id: str) -> None:
        """Raise AuthorizationError if user does not have access to brand."""
        if not await Fetch.verify_brand_ownership(brand_id, user_id):
            raise AuthorizationError("Unauthorized: You do not have access to this brand")

    @staticmethod
    async def create_product_complete(
        title: str,
        description: str,
        vendor: str,
        product_type: str,
        tags: str,
        brand_id: int,
        store_id: int,
        variants: list,
        images: list,
        user_id: str,
        idempotency_key: str = None
    ) -> dict:
        """Create product, sync to Shopify, and persist mapping with strict brand isolation.

        Enforces idempotency by (idempotency_key, user_id, brand_id).
        """
        # Ownership + idempotency
        await ProductService.verify_brand_ownership(brand_id, user_id)
        existing = await ProductService._get_or_create_idempotency(idempotency_key, user_id, brand_id)
        if existing is not None:
            # Conflict / replay -- return stored payload
            raise IdempotencyConflict(json.dumps(existing))

        # Validate images
        invalid_images = ProductService.validate_image_urls(images)
        if invalid_images:
            raise ValidationError(json.dumps({"images": invalid_images}))

        # Build Shopify product input
        product_input = {
            "title": title,
            "descriptionHtml": description,
            "vendor": vendor,
            "productType": product_type,
            "tags": tags.split(",") if tags else [],
        }
        if variants:
            product_input["variants"] = [
                {
                    "sku": v.get("sku"),
                    "price": str(v.get("price")) if v.get("price") is not None else None,
                    "compareAtPrice": str(v.get("compare_at_price")) if v.get("compare_at_price") else None,
                    "weight": v.get("weight"),
                    "weightUnit": v.get("weight_unit", "KG").upper()
                }
                for v in variants
            ]

        # Get Shopify client
        store_config = await get_store_config(store_id, user_id)
        shopify_client = store_config["client"]

        # Create on Shopify
        shopify_product = ProductService._retry_shopify_call(
            lambda: shopify_client.create_product_with_variants(product_input)
        )

        # Upload images
        shopify_images = []
        if images:
            for img in images:
                media = ProductService._retry_shopify_call(
                    lambda: shopify_client.create_product_media(
                        product_id=shopify_product["id"],
                        image_url=img.get("image_url"),
                        alt_text=img.get("alt_text", "Product image")
                    ))
                shopify_images.append({
                    "shopify_media_id": media["id"],
                    "position": img.get("position", len(shopify_images)),
                    "image_url": img.get("image_url"),
                    "alt_text": img.get("alt_text")
                })
        if shopify_images:
            media_ids = [img["shopify_media_id"] for img in sorted(shopify_images, key=lambda x: x["position"])]
            ProductService._retry_shopify_call(
                lambda: shopify_client.reorder_product_media(shopify_product["id"], media_ids)
            )

        # Persist locally
        uid = str(uuid.uuid4())
        await Write.create_product(
            uid=uid,
            brand_id=brand_id,
            title=title,
            description=description,
            vendor=vendor,
            product_type=product_type,
            tags=tags,
            status="ACTIVE",
            price=variants[0].get("price") if variants else 0,
            compare_at_price=variants[0].get("compare_at_price") if variants else None,
            sku=variants[0].get("sku") if variants else None,
            weight=variants[0].get("weight") if variants else None,
            weight_unit=variants[0].get("weight_unit") if variants else None
        )

        # Insert images
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    for img_data in shopify_images:
                        await cursor.execute(
                            '''INSERT INTO low_resol_images (uid, image_url, position, alt_text) VALUES (%s, %s, %s, %s)''',
                            (uid, img_data["image_url"], img_data["position"], img_data["alt_text"])
                        )

                    # Insert Shopify mapping including brand_id and synced_at
                    await cursor.execute(
                        '''INSERT INTO shopify_product_mapping (uid, brand_id, shopify_product_id, store_id, last_sync_status, synced_at) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)''',
                        (uid, brand_id, shopify_product["id"], store_id, "SUCCESS")
                    )
                    await conn.commit()

                    # Record change stack
                    await cursor.execute(
                        '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changed_attribute, update_date, update_time) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME)''',
                        (uid, brand_id, user_id, "CREATE", json.dumps({
                            "shopify_product_id": shopify_product["id"],
                            "images_count": len(shopify_images)
                        }))
                    )
                    await conn.commit()

                    result = {
                        "uid": uid,
                        "shopify_product_id": shopify_product["id"],
                        "images_count": len(shopify_images),
                        "status": "SUCCESS"
                    }
                    # Store idempotency response if key provided
                    await ProductService._store_idempotency_response(idempotency_key, user_id, brand_id, result)
                    return result

                except Exception:
                    await conn.rollback()
                    raise

    @staticmethod
    def validate_image_urls(images: list) -> list:
        """Validate image URLs by issuing HEAD requests."""
        invalid = []
        for img in images or []:
            url = (img or {}).get("image_url")
            if not url:
                invalid.append({"image_url": None, "reason": "missing"})
                continue
            try:
                response = requests.head(url, timeout=5, allow_redirects=True)
                if response.status_code >= 400:
                    invalid.append({"image_url": url, "reason": f"status_{response.status_code}"})
            except Exception:
                invalid.append({"image_url": url, "reason": "unreachable"})
        return invalid

    @staticmethod
    def _retry_shopify_call(callable_fn, attempts: int = 3, backoff_seconds: float = 2.0):
        """Basic retry for Shopify calls on transient errors."""
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                return callable_fn()
            except (requests.Timeout, requests.ConnectionError, ShopifyRetryableError) as exc:
                last_error = exc
                time.sleep(backoff_seconds * attempt)
            except Exception:
                raise
        raise Exception(f"Shopify call failed after retries: {str(last_error)}")

    @staticmethod
    async def _get_or_create_idempotency(idempotency_key: str, user_id: str, brand_id: int) -> dict:
        """Retrieve stored idempotent response or return None.

        This filters by idempotency_key, user_id AND brand_id to ensure strict isolation.
        """
        if not idempotency_key:
            return None
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(
                    '''
                    SELECT response_json FROM catalogue_idempotency
                    WHERE idempotency_key = %s AND user_id = %s AND brand_id = %s
                    ''',
                    (idempotency_key, user_id, brand_id)
                )
                result = await cursor.fetchone()
                if result:
                    return json.loads(result['response_json'])
                return None

    @staticmethod
    async def _store_idempotency_response(idempotency_key: str, user_id: str, brand_id: int, response_payload: dict):
        """Persist idempotency response keyed by idempotency_key+user+brand."""
        if not idempotency_key:
            return
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute(
                        '''
                        INSERT INTO catalogue_idempotency (idempotency_key, user_id, brand_id, response_json)
                        VALUES (%s, %s, %s, %s)
                        ''',
                        (idempotency_key, user_id, brand_id, json.dumps(response_payload))
                    )
                    await conn.commit()
                except Exception:
                    await conn.rollback()

    @staticmethod
    async def update_product(uid: str, brand_id: int, user_id: str, payload: dict) -> dict:
        """Update product and sync to Shopify. Enforces join with uid_record and brand filtering."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await ProductService.verify_brand_ownership(brand_id, user_id)

                    # Ensure product belongs to brand via uid_record join
                    await cursor.execute(
                        '''SELECT spm.store_id, spm.shopify_product_id FROM shopify_product_mapping spm JOIN uid_record u ON spm.uid = u.uid WHERE spm.uid = %s AND u.brand_id = %s''',
                        (uid, brand_id)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        return {"status": "error", "message": "Product mapping not found"}
                    store_id, shopify_product_id = row['store_id'], row['shopify_product_id']
                    store_config = await get_store_config(store_id, user_id)
                    shopify_client = store_config["client"]

                    product_input = {}
                    for field, key in [
                        ("title", "title"),
                        ("description", "descriptionHtml"),
                        ("vendor", "vendor"),
                        ("product_type", "productType"),
                        ("tags", "tags")
                    ]:
                        if field in payload and payload[field] is not None:
                            if field == "tags":
                                product_input[key] = payload[field].split(",") if payload[field] else []
                            else:
                                product_input[key] = payload[field]
                    if product_input:
                        shopify_client.update_product(shopify_product_id, product_input)

                    # Update local DB WITH brand isolation by joining uid_record
                    updates = []
                    params = []
                    allowed_fields = ["title", "description", "vendor", "product_type", "tags", "status", "price", "compare_at_price", "sku", "barcode", "weight", "weight_unit", "collections", "brand_color", "product_remark", "series_length_ankle", "series_rise_waist", "series_knee", "gender", "fit_type", "print_type", "material", "material_composition", "care_instruction", "art_technique", "stitch_type"]
                    for key in allowed_fields:
                        if key in payload:
                            updates.append(f'{key} = %s')
                            params.append(payload[key])

                    if updates:
                        params.extend([uid, brand_id])
                        await cursor.execute(f"UPDATE fashion SET {', '.join(updates)} WHERE uid = %s AND brand_id = %s", params)
                        await conn.commit()

                        # Audit/stack insert with changed_attribute JSON and timestamps
                        await cursor.execute(
                            '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changed_attribute, update_date, update_time) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME)''',
                            (uid, brand_id, user_id, "UPDATE", json.dumps(payload))
                        )
                        await conn.commit()

                    return {"status": "success", "uid": uid}
                except Exception as e:
                    await conn.rollback()
                    return {"status": "error", "message": str(e)}

    @staticmethod
    async def delete_product(uid: str, brand_id: int, user_id: str, soft_delete: bool = True) -> dict:
        """Delete or archive product and delete on Shopify. Enforces uid_record brand join."""
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await ProductService.verify_brand_ownership(brand_id, user_id)
                    await cursor.execute(
                        '''SELECT spm.store_id, spm.shopify_product_id FROM shopify_product_mapping spm JOIN uid_record u ON spm.uid = u.uid WHERE spm.uid = %s AND u.brand_id = %s''',
                        (uid, brand_id)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        return {"status": "error", "message": "Product mapping not found"}
                    store_id, shopify_product_id = row['store_id'], row['shopify_product_id']
                    store_config = await get_store_config(store_id, user_id)
                    shopify_client = store_config["client"]
                    shopify_client.delete_product(shopify_product_id)
                    if soft_delete:
                        await cursor.execute(
                            '''UPDATE fashion SET status = 'ARCHIVED' WHERE uid = %s AND brand_id = %s''',
                            (uid, brand_id)
                        )
                    else:
                        await cursor.execute(
                            '''DELETE f FROM fashion f JOIN uid_record u ON f.uid = u.uid WHERE f.uid = %s AND u.brand_id = %s''',
                            (uid, brand_id)
                        )
                    await conn.commit()
                    await cursor.execute(
                        '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changed_attribute, update_date, update_time) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME)''',
                        (uid, brand_id, user_id, "DELETE", json.dumps({"soft_delete": soft_delete}))
                    )
                    await conn.commit()
                    return {"status": "success", "uid": uid}
                except Exception as e:
                    await conn.rollback()
                    return {"status": "error", "message": str(e)}

    @staticmethod
    async def get_product_by_uid(uid: str, brand_id: int, user_id: str) -> dict:
        """Retrieve product details by uid ensuring brand isolation by joining uid_record."""
        await ProductService.verify_brand_ownership(brand_id, user_id)
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute('''
                        SELECT f.uid, u.brand_id, f.title, f.description, f.vendor, f.product_type, f.tags,
                               f.status, f.price, f.compare_at_price, f.sku, f.barcode, f.weight, f.weight_unit,
                               f.collections, f.brand_color, f.product_remark, f.series_length_ankle,
                               f.series_rise_waist, f.series_knee, f.gender, f.fit_type, f.print_type,
                               f.material, f.material_composition, f.care_instruction, f.art_technique,
                               f.stitch_type, f.created_at, f.updated_at
                        FROM fashion f
                        JOIN uid_record u ON f.uid = u.uid
                        WHERE f.uid = %s AND u.brand_id = %s
                    ''', (uid, brand_id))
                    result = await cursor.fetchone()
                    if not result:
                        return None
                    product = {
                        'uid': result['uid'],
                        'brand_id': result['brand_id'],
                        'title': result['title'],
                        'description': result['description'],
                        'vendor': result['vendor'],
                        'product_type': result['product_type'],
                        'tags': result['tags'],
                        'status': result['status'],
                        'price': result['price'],
                        'compare_at_price': result['compare_at_price'],
                        'sku': result['sku'],
                        'barcode': result['barcode'],
                        'weight': result['weight'],
                        'weight_unit': result['weight_unit'],
                        'collections': result['collections'],
                        'brand_color': result['brand_color'],
                        'product_remark': result['product_remark'],
                        'series_length_ankle': result['series_length_ankle'],
                        'series_rise_waist': result['series_rise_waist'],
                        'series_knee': result['series_knee'],
                        'gender': result['gender'],
                        'fit_type': result['fit_type'],
                        'print_type': result['print_type'],
                        'material': result['material'],
                        'material_composition': result['material_composition'],
                        'care_instruction': result['care_instruction'],
                        'art_technique': result['art_technique'],
                        'stitch_type': result['stitch_type'],
                        'created_at': result['created_at'],
                        'updated_at': result['updated_at']
                    }
                    return product
                except Exception:
                    return None

    @staticmethod
    async def list_products(brand_id: int, user_id: str, limit: int = 50, offset: int = 0, status: str = None, search: str = None) -> list:
        """List products for a brand with strict join to uid_record and brand filtering."""
        await ProductService.verify_brand_ownership(brand_id, user_id)
        pool = current_app.pool
        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    where_clauses = ["u.brand_id = %s"]
                    params = [brand_id]

                    if status:
                        where_clauses.append("f.status = %s")
                        params.append(status.upper())

                    if search:
                        where_clauses.append("(f.title LIKE %s OR f.vendor LIKE %s OR f.sku LIKE %s)")
                        search_term = f"%{search}%"
                        params.extend([search_term, search_term, search_term])

                    query = f'''
                        SELECT f.uid, u.brand_id, f.title, f.price, f.vendor, f.status,
                               f.created_at, COUNT(DISTINCT li.id) as image_count
                        FROM fashion f
                        JOIN uid_record u ON f.uid = u.uid
                        LEFT JOIN low_resol_images li ON f.uid = li.uid
                        WHERE {' AND '.join(where_clauses)}
                        GROUP BY f.uid
                        ORDER BY f.created_at DESC
                        LIMIT %s OFFSET %s
                    '''
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

                except Exception:
                    return []
