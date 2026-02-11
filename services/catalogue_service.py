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
    def verify_brand_ownership(brand_id: int, user_id: str) -> bool:
        """Verify that a user has access to a brand."""
        from database import Fetch
        return Fetch.verify_brand_ownership(brand_id, user_id)

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
        user_id: str
    ) -> dict:
        """
        Complete product creation with variants and images, brand-centric.
        """
        from database import Write, Fetch
        # STEP 1: Verify brand ownership
        if not CatalogueService.verify_brand_ownership(brand_id, user_id):
            raise AuthorizationError("Unauthorized: You do not have access to this brand")

        # STEP 2: Get Shopify client
        store_config = get_store_config(store_id, user_id)
        shopify_client = store_config["client"]

        # STEP 3: Validate image URLs
        invalid_images = CatalogueService.validate_image_urls(images)
        if invalid_images:
            raise ValidationError(json.dumps({"images": invalid_images}))

        # STEP 4: Build product input
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
                    "price": str(v.get("price")),
                    "compareAtPrice": str(v.get("compare_at_price")) if v.get("compare_at_price") else None,
                    "weight": v.get("weight"),
                    "weightUnit": v.get("weight_unit", "KG").upper()
                }
                for v in variants
            ]

        # STEP 5: Create on Shopify
        shopify_product = CatalogueService._retry_shopify_call(
            lambda: shopify_client.create_product_with_variants(product_input)
        )

        # STEP 6: Upload images
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
        if shopify_images:
            media_ids = [img["shopify_media_id"] for img in sorted(shopify_images, key=lambda x: x["position"])]
            CatalogueService._retry_shopify_call(
                lambda: shopify_client.reorder_product_media(shopify_product["id"], media_ids)
            )

        # STEP 7: Save to DB (fashion, low_resol_images, shopify_product_mapping)
        import uuid
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
        for img_data in shopify_images:
            cursor.execute(
                '''INSERT INTO low_resol_images (uid, image_url, position, alt_text) VALUES (%s, %s, %s, %s)''',
                (uid, img_data["image_url"], img_data["position"], img_data["alt_text"])
            )
        # Insert Shopify mapping
        cursor.execute(
            '''INSERT INTO shopify_product_mapping (uid, shopify_product_id, store_id, last_sync_status) VALUES (%s, %s, %s, %s)''',
            (uid, shopify_product["id"], store_id, "SUCCESS")
        )
        mysql.connection.commit()
        # Audit log
        cursor.execute(
            '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changes) VALUES (%s, %s, %s, %s, %s)''',
            (uid, brand_id, user_id, "CREATE", json.dumps({
                "shopify_product_id": shopify_product["id"],
                "images_count": len(shopify_images)
            }))
        )
        mysql.connection.commit()
        cursor.close()
        return {
            "uid": uid,
            "shopify_product_id": shopify_product["id"],
            "images_count": len(shopify_images),
            "status": "SUCCESS"
        }

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
    def update_product(uid: str, brand_id: int, user_id: str, payload: dict) -> dict:
        """Update product and sync to Shopify."""
        cursor = mysql.connection.cursor()
        try:
            # Ownership check
            if not CatalogueService.verify_brand_ownership(brand_id, user_id):
                return {"status": "error", "message": "Unauthorized"}
            cursor.execute(
                '''SELECT spm.store_id, spm.shopify_product_id FROM shopify_product_mapping spm WHERE spm.uid = %s''',
                (uid,)
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
            # Update local DB
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
            # Audit log
            cursor.execute(
                '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changes) VALUES (%s, %s, %s, %s, %s)''',
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
        """Delete or archive product and delete on Shopify."""
        cursor = mysql.connection.cursor()
        try:
            if not CatalogueService.verify_brand_ownership(brand_id, user_id):
                return {"status": "error", "message": "Unauthorized"}
            cursor.execute(
                '''SELECT spm.store_id, spm.shopify_product_id FROM shopify_product_mapping spm WHERE spm.uid = %s''',
                (uid,)
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
                    '''DELETE FROM fashion WHERE uid = %s AND brand_id = %s''',
                    (uid, brand_id)
                )
            mysql.connection.commit()
            cursor.execute(
                '''INSERT INTO product_info_change_stack (uid, brand_id, user_id, action, changes) VALUES (%s, %s, %s, %s, %s)''',
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
        """Retrieve product details by UID and brand, with ownership check."""
        from database import Fetch
        if not CatalogueService.verify_brand_ownership(brand_id, user_id):
            return None
        return Fetch.get_product_by_uid(uid, brand_id)

    @staticmethod
    def list_products(brand_id: int, user_id: str, limit: int = 50, offset: int = 0, status: str = None, search: str = None) -> list:
        """List products for a brand with optional filtering and ownership check."""
        from database import Fetch
        if not CatalogueService.verify_brand_ownership(brand_id, user_id):
            return []
        return Fetch.list_products(brand_id, limit, offset, status, search)

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
