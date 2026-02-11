import json
import uuid
import time
import requests
from database import mysql, Fetch, Write
from services.shopify_graphql import ShopifyRetryableError
from services.shopify_helpers import get_store_config
from services.exceptions import AuthorizationError, ShopifyAPIError, ValidationError, IdempotencyConflict


class ProductService:
    """Service for managing products (brand-centric) with Shopify sync and strict isolation."""

    @staticmethod
    def verify_brand_ownership(brand_id: int, user_id: str) -> None:
        """Raise AuthorizationError if user does not have access to brand."""
        if not Fetch.verify_brand_ownership(brand_id, user_id):
            raise AuthorizationError("Unauthorized: You do not have access to this brand")

    @staticmethod
    def create_product_complete(
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
        ProductService.verify_brand_ownership(brand_id, user_id)
        existing = ProductService._get_or_create_idempotency(idempotency_key, user_id, brand_id)
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
        store_config = get_store_config(store_id, user_id)
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
        Write.create_product(
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
        cursor = mysql.connection.cursor()
        try:
            for img_data in shopify_images:
                cursor.execute(
                    '''INSERT INTO low_resol_images (uid, image_url, position, alt_text) VALUES (%s, %s, %s, %s)''',
                    (uid, img_data["image_url"], img_data["position"], img_data["alt_text"])
                )

            # Insert Shopify mapping including brand_id and synced_at
            cursor.execute(
                '''INSERT INTO shopify_product_mapping (uid, brand_id, shopify_product_id, store_id, last_sync_status, synced_at) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)''',
                (uid, brand_id, shopify_product["id"], store_id, "SUCCESS")
            )
            mysql.connection.commit()

            # Record change stack
            cursor.execute(
                '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changed_attribute, update_date, update_time) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME)''',
                (uid, brand_id, user_id, "CREATE", json.dumps({
                    "shopify_product_id": shopify_product["id"],
                    "images_count": len(shopify_images)
                }))
            )
            mysql.connection.commit()

            result = {
                "uid": uid,
                "shopify_product_id": shopify_product["id"],
                "images_count": len(shopify_images),
                "status": "SUCCESS"
            }
            # Store idempotency response if key provided
            ProductService._store_idempotency_response(idempotency_key, user_id, brand_id, result)
            return result

        except Exception as e:
            mysql.connection.rollback()
            raise
        finally:
            cursor.close()

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
    def _get_or_create_idempotency(idempotency_key: str, user_id: str, brand_id: int) -> dict:
        """Retrieve stored idempotent response or return None.

        This filters by idempotency_key, user_id AND brand_id to ensure strict isolation.
        """
        if not idempotency_key:
            return None
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                '''
                SELECT response_json FROM catalogue_idempotency
                WHERE idempotency_key = %s AND user_id = %s AND brand_id = %s
                ''',
                (idempotency_key, user_id, brand_id)
            )
            result = cursor.fetchone()
            if result:
                return json.loads(result[0])
            return None
        finally:
            cursor.close()

    @staticmethod
    def _store_idempotency_response(idempotency_key: str, user_id: str, brand_id: int, response_payload: dict):
        """Persist idempotency response keyed by idempotency_key+user+brand."""
        if not idempotency_key:
            return
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                '''
                INSERT INTO catalogue_idempotency (idempotency_key, user_id, brand_id, response_json)
                VALUES (%s, %s, %s, %s)
                ''',
                (idempotency_key, user_id, brand_id, json.dumps(response_payload))
            )
            mysql.connection.commit()
        except Exception:
            mysql.connection.rollback()
        finally:
            cursor.close()

    @staticmethod
    def update_product(uid: str, brand_id: int, user_id: str, payload: dict) -> dict:
        """Update product and sync to Shopify. Enforces join with uid_record and brand filtering."""
        cursor = mysql.connection.cursor()
        try:
            ProductService.verify_brand_ownership(brand_id, user_id)

            # Ensure product belongs to brand via uid_record join
            cursor.execute(
                '''SELECT spm.store_id, spm.shopify_product_id FROM shopify_product_mapping spm JOIN uid_record u ON spm.uid = u.uid WHERE spm.uid = %s AND u.brand_id = %s''',
                (uid, brand_id)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "Product mapping not found"}
            store_id, shopify_product_id = row
            store_config = get_store_config(store_id, user_id)
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
                cursor.execute(f"UPDATE fashion SET {', '.join(updates)} WHERE uid = %s AND brand_id = %s", params)
                mysql.connection.commit()

                # Audit/stack insert with changed_attribute JSON and timestamps
                cursor.execute(
                    '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changed_attribute, update_date, update_time) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME)''',
                    (uid, brand_id, user_id, "UPDATE", json.dumps(payload))
                )
                mysql.connection.commit()

            return {"status": "success", "uid": uid}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()

    @staticmethod
    def delete_product(uid: str, brand_id: int, user_id: str, soft_delete: bool = True) -> dict:
        """Delete or archive product and delete on Shopify. Enforces uid_record brand join."""
        cursor = mysql.connection.cursor()
        try:
            ProductService.verify_brand_ownership(brand_id, user_id)
            cursor.execute(
                '''SELECT spm.store_id, spm.shopify_product_id FROM shopify_product_mapping spm JOIN uid_record u ON spm.uid = u.uid WHERE spm.uid = %s AND u.brand_id = %s''',
                (uid, brand_id)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "Product mapping not found"}
            store_id, shopify_product_id = row
            store_config = get_store_config(store_id, user_id)
            shopify_client = store_config["client"]
            shopify_client.delete_product(shopify_product_id)
            if soft_delete:
                cursor.execute(
                    '''UPDATE fashion SET status = 'ARCHIVED' WHERE uid = %s AND brand_id = %s''',
                    (uid, brand_id)
                )
            else:
                cursor.execute(
                    '''DELETE f FROM fashion f JOIN uid_record u ON f.uid = u.uid WHERE f.uid = %s AND u.brand_id = %s''',
                    (uid, brand_id)
                )
            mysql.connection.commit()
            cursor.execute(
                '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changed_attribute, update_date, update_time) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE, CURRENT_TIME)''',
                (uid, brand_id, user_id, "DELETE", json.dumps({"soft_delete": soft_delete}))
            )
            mysql.connection.commit()
            return {"status": "success", "uid": uid}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()

    @staticmethod
    def get_product_by_uid(uid: str, brand_id: int, user_id: str) -> dict:
        """Retrieve product details by uid ensuring brand isolation by joining uid_record."""
        ProductService.verify_brand_ownership(brand_id, user_id)
        cursor = mysql.connection.cursor()
        try:
            cursor.execute('''
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
            result = cursor.fetchone()
            if not result:
                return None
            product = {
                'uid': result[0],
                'brand_id': result[1],
                'title': result[2],
                'description': result[3],
                'vendor': result[4],
                'product_type': result[5],
                'tags': result[6],
                'status': result[7],
                'price': result[8],
                'compare_at_price': result[9],
                'sku': result[10],
                'barcode': result[11],
                'weight': result[12],
                'weight_unit': result[13],
                'collections': result[14],
                'brand_color': result[15],
                'product_remark': result[16],
                'series_length_ankle': result[17],
                'series_rise_waist': result[18],
                'series_knee': result[19],
                'gender': result[20],
                'fit_type': result[21],
                'print_type': result[22],
                'material': result[23],
                'material_composition': result[24],
                'care_instruction': result[25],
                'art_technique': result[26],
                'stitch_type': result[27],
                'created_at': result[28],
                'updated_at': result[29]
            }
            return product
        except Exception as e:
            return None
        finally:
            cursor.close()

    @staticmethod
    def list_products(brand_id: int, user_id: str, limit: int = 50, offset: int = 0, status: str = None, search: str = None) -> list:
        """List products for a brand with strict join to uid_record and brand filtering."""
        ProductService.verify_brand_ownership(brand_id, user_id)
        cursor = mysql.connection.cursor()
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
            cursor.execute(query, tuple(params))

            results = cursor.fetchall()
            return [
                {
                    'uid': row[0],
                    'brand_id': row[1],
                    'title': row[2],
                    'price': row[3],
                    'vendor': row[4],
                    'status': row[5],
                    'created_at': row[6],
                    'images_count': row[7]
                }
                for row in results
            ]

        except Exception as e:
            return []
        finally:
            cursor.close()
