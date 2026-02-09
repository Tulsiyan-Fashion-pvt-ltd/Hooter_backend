import json
import uuid
import time
import requests
from database import mysql, Fetch
from services.shopify_graphql import ShopifyRetryableError
from services.shopify_helpers import get_store_config
from services.exceptions import AuthorizationError, ShopifyAPIError, ValidationError


class CatalogueService:
    """Service for managing catalogue operations with Shopify sync and multi-client support."""

    @staticmethod
    def verify_store_ownership(store_id: int, user_id: str) -> bool:
        """Verify that a user owns a specific store."""
        cursor = None
        try:
            cursor = mysql.connection.cursor()
            cursor.execute(
                'SELECT store_id FROM stores WHERE store_id = %s AND user_id = %s AND is_active = TRUE',
                (store_id, user_id)
            )
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            print(f'Error verifying store ownership: {str(e)}')
            return False
        finally:
            if cursor:
                cursor.close()

    @staticmethod
    def create_catalogue_complete(
        title: str,
        description: str,
        vendor: str,
        product_type: str,
        tags: str,
        store_id: int,
        variants: list,
        images: list,
        user_id: str
    ) -> dict:
        """
        Complete product creation with variants and images.
        
        Args:
            title: Product title
            description: Product description
            vendor: Product vendor/manufacturer
            product_type: Product category
            tags: Comma-separated tags
            store_id: Shopify store ID
            variants: List of variant dicts with sku, price, etc.
            images: List of image dicts with image_url, alt_text, position
            user_id: User ID who owns the store
        
        Returns:
            dict with catalogue_id, shopify_product_id, variants_count, images_count
        """
        cursor = None
        try:
            # STEP 1: Verify ownership
            if not CatalogueService.verify_store_ownership(store_id, user_id):
                raise AuthorizationError("Unauthorized: You do not own this store")
            
            # STEP 2-4: Get store config + Shopify client
            store_config = get_store_config(store_id, user_id)
            shopify_client = store_config["client"]
            store = store_config["store"]

            # STEP 4.1: Validate image URLs (if provided)
            invalid_images = CatalogueService.validate_image_urls(images)
            if invalid_images:
                raise ValidationError(json.dumps({"images": invalid_images}))
            
            # STEP 5: Build product input with variants
            product_input = {
                "title": title,
                "descriptionHtml": description,
                "vendor": vendor,
                "productType": product_type,
                "tags": tags.split(",") if tags else [],
            }
            
            # Add variants if provided
            if variants:
                product_input["variants"] = [
                    {
                        "sku": v.get("sku"),
                        "price": str(v.get("price")),
                        "compareAtPrice": str(v.get("compare_at_price")) if v.get("compare_at_price") else None,
                        "weight": v.get("weight"),
                        "weightUnit": v.get("weight_unit", "KG").upper()
                    }
                    for v in variants
                ]
            
            # STEP 6: Create on Shopify with variants (retry on transient errors)
            shopify_product = CatalogueService._retry_shopify_call(
                lambda: shopify_client.create_product_with_variants(product_input)
            )
            
            # STEP 7: Upload images
            shopify_images = []
            if images:
                for img in images:
                    media = CatalogueService._retry_shopify_call(
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

            # Optional: reorder images to desired positions
            if shopify_images:
                media_ids = [img["shopify_media_id"] for img in sorted(shopify_images, key=lambda x: x["position"])]
                CatalogueService._retry_shopify_call(
                    lambda: shopify_client.reorder_product_media(shopify_product["id"], media_ids)
                )
            
            # STEP 8: Save to database
            cursor = mysql.connection.cursor()
            catalogue_id = str(uuid.uuid4())
            
            try:
                # Insert base product
                cursor.execute(
                    """
                    INSERT INTO catalogue (
                        catalogue_id, store_id, user_id, title, description,
                        vendor, product_type, tags, status, price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        catalogue_id, store_id, user_id, title, description,
                        vendor, product_type, tags, "ACTIVE",
                        variants[0].get("price") if variants else 0
                    )
                )
                
                # Insert Shopify mapping
                cursor.execute(
                    """
                    INSERT INTO catalogue_shopify_mapping (
                        catalogue_id, shopify_product_id, store_id, last_sync_status
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (catalogue_id, shopify_product["id"], store_id, "SUCCESS")
                )
                
                # Insert variants
                for shopify_variant in shopify_product["variants"]:
                    variant_id = str(uuid.uuid4())
                    cursor.execute(
                        """
                        INSERT INTO catalogue_variants (
                            variant_id, catalogue_id, shopify_variant_id,
                            sku, price, inventory_item_id, status
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            variant_id,
                            catalogue_id,
                            shopify_variant["id"],
                            shopify_variant.get("sku"),
                            shopify_variant.get("price", 0),
                            shopify_variant["inventoryItem"]["id"],
                            "ACTIVE"
                        )
                    )
                
                # Insert images
                for img_data in shopify_images:
                    image_id = str(uuid.uuid4())
                    cursor.execute(
                        """
                        INSERT INTO catalogue_images (
                            image_id, catalogue_id, shopify_media_id,
                            image_url, alt_text, position, uploaded_by
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            image_id,
                            catalogue_id,
                            img_data["shopify_media_id"],
                            img_data["image_url"],
                            img_data["alt_text"],
                            img_data["position"],
                            user_id
                        )
                    )
                
                mysql.connection.commit()

                CatalogueService._log_audit(
                    catalogue_id=catalogue_id,
                    store_id=store_id,
                    user_id=user_id,
                    action="CREATE",
                    changes={
                        "shopify_product_id": shopify_product["id"],
                        "variants_count": len(shopify_product["variants"]),
                        "images_count": len(shopify_images)
                    }
                )
                
                return {
                    "catalogue_id": catalogue_id,
                    "shopify_product_id": shopify_product["id"],
                    "variants_count": len(shopify_product["variants"]),
                    "images_count": len(shopify_images),
                    "status": "SUCCESS"
                }
            
            except Exception as db_error:
                mysql.connection.rollback()
                raise Exception(f"Database operation failed: {str(db_error)}")
        
        except Exception:
            raise
        
        finally:
            if cursor:
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
    def _log_audit(catalogue_id: str, store_id: int, user_id: str, action: str, changes: dict):
        """Insert audit log entry."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO catalogue_audit_log (catalogue_id, store_id, user_id, action, changes)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (catalogue_id, store_id, user_id, action, json.dumps(changes or {}))
            )
            mysql.connection.commit()
        except Exception as e:
            mysql.connection.rollback()
            print(f"Failed to log audit entry: {str(e)}")
        finally:
            cursor.close()

    @staticmethod
    def _get_or_create_idempotency(idempotency_key: str, user_id: str, store_id: int) -> dict:
        """Retrieve idempotent response or None."""
        if not idempotency_key:
            return None
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT response_json FROM catalogue_idempotency
                WHERE idempotency_key = %s AND user_id = %s AND store_id = %s
                """,
                (idempotency_key, user_id, store_id)
            )
            result = cursor.fetchone()
            if result:
                return json.loads(result[0])
            return None
        finally:
            cursor.close()

    @staticmethod
    def _store_idempotency_response(idempotency_key: str, user_id: str, store_id: int, response_payload: dict):
        """Persist idempotency response if key provided."""
        if not idempotency_key:
            return
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO catalogue_idempotency (idempotency_key, user_id, store_id, response_json)
                VALUES (%s, %s, %s, %s)
                """,
                (idempotency_key, user_id, store_id, json.dumps(response_payload))
            )
            mysql.connection.commit()
        except Exception:
            mysql.connection.rollback()
        finally:
            cursor.close()

    @staticmethod
    def sync_inventory_for_catalogue(catalogue_id: str, user_id: str, quantities: dict = None) -> dict:
        """Sync inventory for all variants in a catalogue with Shopify."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT c.store_id, m.shopify_product_id
                FROM catalogue c
                LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                WHERE c.catalogue_id = %s AND c.user_id = %s
                """,
                (catalogue_id, user_id)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "Catalogue not found"}
            store_id, shopify_product_id = row

            store_config = get_store_config(store_id, user_id)
            shopify_client = store_config["client"]

            locations = shopify_client.get_locations()
            if not locations:
                return {"status": "error", "message": "No Shopify locations found"}
            location_id = locations[0]["id"]

            cursor.execute(
                """
                SELECT variant_id, inventory_item_id
                FROM catalogue_variants
                WHERE catalogue_id = %s
                """,
                (catalogue_id,)
            )
            variants = cursor.fetchall()

            synced = 0
            for variant_id, inventory_item_id in variants:
                shopify_client.activate_inventory(inventory_item_id, location_id)
                quantity = 0
                if quantities and variant_id in quantities:
                    quantity = quantities[variant_id]
                shopify_client.set_inventory_quantities(inventory_item_id, location_id, quantity)
                cursor.execute(
                    """
                    INSERT INTO catalogue_inventory (variant_id, location_id, available_quantity)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE available_quantity = VALUES(available_quantity),
                        last_synced_at = CURRENT_TIMESTAMP,
                        sync_status = 'IN_SYNC'
                    """,
                    (variant_id, location_id, quantity)
                )
                synced += 1

            mysql.connection.commit()
            CatalogueService._log_audit(
                catalogue_id=catalogue_id,
                store_id=store_id,
                user_id=user_id,
                action="INVENTORY",
                changes={"variants_synced": synced, "location_id": location_id}
            )
            return {"status": "success", "synced": synced, "location_id": location_id}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()

    @staticmethod
    def update_catalogue(catalogue_id: str, user_id: str, payload: dict) -> dict:
        """Update catalogue and sync to Shopify."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT c.store_id, m.shopify_product_id
                FROM catalogue c
                LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                WHERE c.catalogue_id = %s AND c.user_id = %s
                """,
                (catalogue_id, user_id)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "Catalogue not found"}
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

            updates = []
            params = []
            if "title" in payload:
                updates.append("title = %s")
                params.append(payload.get("title"))
            if "description" in payload:
                updates.append("description = %s")
                params.append(payload.get("description"))
            if "vendor" in payload:
                updates.append("vendor = %s")
                params.append(payload.get("vendor"))
            if "product_type" in payload:
                updates.append("product_type = %s")
                params.append(payload.get("product_type"))
            if "tags" in payload:
                updates.append("tags = %s")
                params.append(payload.get("tags"))

            if updates:
                params.extend([catalogue_id, user_id])
                cursor.execute(
                    f"UPDATE catalogue SET {', '.join(updates)} WHERE catalogue_id = %s AND user_id = %s",
                    params
                )

            mysql.connection.commit()
            CatalogueService._log_audit(
                catalogue_id=catalogue_id,
                store_id=store_id,
                user_id=user_id,
                action="UPDATE",
                changes=payload
            )
            return {"status": "success", "catalogue_id": catalogue_id}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()

    @staticmethod
    def delete_catalogue(catalogue_id: str, user_id: str, soft_delete: bool = True) -> dict:
        """Delete or archive catalogue and delete on Shopify."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT c.store_id, m.shopify_product_id
                FROM catalogue c
                LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                WHERE c.catalogue_id = %s AND c.user_id = %s
                """,
                (catalogue_id, user_id)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "Catalogue not found"}
            store_id, shopify_product_id = row

            store_config = get_store_config(store_id, user_id)
            shopify_client = store_config["client"]
            shopify_client.delete_product(shopify_product_id)

            if soft_delete:
                cursor.execute(
                    """
                    UPDATE catalogue SET status = 'ARCHIVED'
                    WHERE catalogue_id = %s AND user_id = %s
                    """,
                    (catalogue_id, user_id)
                )
            else:
                cursor.execute(
                    "DELETE FROM catalogue WHERE catalogue_id = %s AND user_id = %s",
                    (catalogue_id, user_id)
                )

            mysql.connection.commit()
            CatalogueService._log_audit(
                catalogue_id=catalogue_id,
                store_id=store_id,
                user_id=user_id,
                action="DELETE",
                changes={"soft_delete": soft_delete}
            )
            return {"status": "success", "catalogue_id": catalogue_id}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()

    @staticmethod
    def get_catalogue_by_id(catalogue_id: str, user_id: str = None) -> dict:
        """
        Retrieve catalogue details by ID with optional user ownership check.

        Args:
            catalogue_id: UUID of the catalogue
            user_id: Optional user ID to verify ownership

        Returns:
            dict: Catalogue data with Shopify mapping, variants, and images
        """
        cursor = None
        try:
            cursor = mysql.connection.cursor()
            
            if user_id:
                cursor.execute(
                    """
                    SELECT c.catalogue_id, c.store_id, c.title, c.description, c.price,
                           c.vendor, c.product_type, c.tags, c.status, c.user_id, c.created_at,
                           m.shopify_product_id, m.last_sync_status
                    FROM catalogue c
                    LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                    WHERE c.catalogue_id = %s AND c.user_id = %s
                    """,
                    (catalogue_id, user_id)
                )
            else:
                cursor.execute(
                    """
                    SELECT c.catalogue_id, c.store_id, c.title, c.description, c.price,
                           c.vendor, c.product_type, c.tags, c.status, c.user_id, c.created_at,
                           m.shopify_product_id, m.last_sync_status
                    FROM catalogue c
                    LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                    WHERE c.catalogue_id = %s
                    """,
                    (catalogue_id,)
                )

            result = cursor.fetchone()
            if result:
                # Fetch variants for this product
                cursor.execute(
                    """
                    SELECT variant_id, sku, price, title, status
                    FROM catalogue_variants
                    WHERE catalogue_id = %s
                    """,
                    (catalogue_id,)
                )
                variants = [
                    {
                        "variant_id": row[0],
                        "sku": row[1],
                        "price": row[2],
                        "title": row[3],
                        "status": row[4]
                    }
                    for row in cursor.fetchall()
                ]
                
                # Fetch images for this product
                cursor.execute(
                    """
                    SELECT image_id, image_url, alt_text, position
                    FROM catalogue_images
                    WHERE catalogue_id = %s
                    ORDER BY position
                    """,
                    (catalogue_id,)
                )
                images = [
                    {
                        "image_id": row[0],
                        "image_url": row[1],
                        "alt_text": row[2],
                        "position": row[3]
                    }
                    for row in cursor.fetchall()
                ]
                
                return {
                    "catalogue_id": result[0],
                    "store_id": result[1],
                    "title": result[2],
                    "description": result[3],
                    "price": result[4],
                    "vendor": result[5],
                    "product_type": result[6],
                    "tags": result[7],
                    "status": result[8],
                    "user_id": result[9],
                    "created_at": result[10],
                    "shopify_product_id": result[11],
                    "sync_status": result[12],
                    "variants": variants,
                    "images": images
                }
            return None

        finally:
            if cursor:
                cursor.close()

    @staticmethod
    def list_catalogues(
        user_id: str,
        store_id: int = None,
        limit: int = 50,
        offset: int = 0,
        status: str = None,
        search: str = None
    ) -> list:
        """
        Retrieve catalogue list for a user with optional store filtering.

        Args:
            user_id: User ID (required for security)
            store_id: Optional filter by specific store
            limit: Number of records to return
            offset: Pagination offset

        Returns:
            list: Catalogue records with variants and images count
        """
        cursor = None
        try:
            cursor = mysql.connection.cursor()

            where_clauses = ["c.user_id = %s"]
            params = [user_id]

            if store_id:
                where_clauses.append("c.store_id = %s")
                params.append(store_id)

            if status:
                where_clauses.append("c.status = %s")
                params.append(status.upper())

            if search:
                where_clauses.append("(c.title LIKE %s OR c.vendor LIKE %s OR c.tags LIKE %s)")
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])

            query = f"""
                SELECT c.catalogue_id, c.store_id, c.title, c.price,
                       c.vendor, c.status, c.user_id, c.created_at,
                       m.shopify_product_id,
                       COUNT(DISTINCT cv.variant_id) as variant_count,
                       COUNT(DISTINCT ci.image_id) as image_count
                FROM catalogue c
                LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                LEFT JOIN catalogue_variants cv ON c.catalogue_id = cv.catalogue_id
                LEFT JOIN catalogue_images ci ON c.catalogue_id = ci.catalogue_id
                WHERE {' AND '.join(where_clauses)}
                GROUP BY c.catalogue_id
                ORDER BY c.created_at DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, offset])
            cursor.execute(query, tuple(params))

            results = cursor.fetchall()
            return [
                {
                    "catalogue_id": row[0],
                    "store_id": row[1],
                    "title": row[2],
                    "price": row[3],
                    "vendor": row[4],
                    "status": row[5],
                    "user_id": row[6],
                    "created_at": row[7],
                    "shopify_product_id": row[8],
                    "variants_count": row[9],
                    "images_count": row[10]
                }
                for row in results
            ]

        finally:
            if cursor:
                cursor.close()

    @staticmethod
    def update_catalogue_from_webhook(payload: dict) -> dict:
        """Update catalogue from Shopify product/update webhook payload."""
        cursor = mysql.connection.cursor()
        try:
            shopify_product_id = str(payload.get("id"))
            cursor.execute(
                """
                SELECT c.catalogue_id, c.store_id, c.user_id
                FROM catalogue_shopify_mapping m
                JOIN catalogue c ON c.catalogue_id = m.catalogue_id
                WHERE m.shopify_product_id = %s
                """,
                (shopify_product_id,)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "Product mapping not found"}
            catalogue_id, store_id, user_id = row

            title = payload.get("title")
            description = payload.get("body_html")
            vendor = payload.get("vendor")
            product_type = payload.get("product_type")
            tags = payload.get("tags")

            cursor.execute(
                """
                UPDATE catalogue SET title = %s, description = %s, vendor = %s,
                    product_type = %s, tags = %s
                WHERE catalogue_id = %s
                """,
                (title, description, vendor, product_type, tags, catalogue_id)
            )
            mysql.connection.commit()
            CatalogueService._log_audit(
                catalogue_id=catalogue_id,
                store_id=store_id,
                user_id=user_id,
                action="SYNC",
                changes={
                    "source": "shopify_webhook",
                    "shopify_product_id": shopify_product_id
                }
            )
            return {"status": "success", "catalogue_id": catalogue_id}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()

    @staticmethod
    def update_inventory_from_webhook(payload: dict) -> dict:
        """Update inventory from Shopify inventory level webhook payload."""
        cursor = mysql.connection.cursor()
        try:
            inventory_item_id = payload.get("inventory_item_id")
            location_id = payload.get("location_id")
            available = payload.get("available")
            if inventory_item_id is None or location_id is None:
                return {"status": "error", "message": "missing inventory_item_id or location_id"}

            cursor.execute(
                """
                SELECT variant_id, catalogue_id
                FROM catalogue_variants
                WHERE inventory_item_id = %s
                """,
                (inventory_item_id,)
            )
            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "variant not found"}
            variant_id, catalogue_id = row

            cursor.execute(
                """
                INSERT INTO catalogue_inventory (variant_id, location_id, available_quantity)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE available_quantity = VALUES(available_quantity),
                    last_synced_at = CURRENT_TIMESTAMP,
                    sync_status = 'IN_SYNC'
                """,
                (variant_id, str(location_id), int(available) if available is not None else 0)
            )
            mysql.connection.commit()
            CatalogueService._log_audit(
                catalogue_id=catalogue_id,
                store_id=None,
                user_id=None,
                action="INVENTORY",
                changes={
                    "source": "shopify_webhook",
                    "inventory_item_id": inventory_item_id,
                    "location_id": location_id,
                    "available": available
                }
            )
            return {"status": "success", "variant_id": variant_id, "catalogue_id": catalogue_id}
        except Exception as e:
            mysql.connection.rollback()
            return {"status": "error", "message": str(e)}
        finally:
            cursor.close()
