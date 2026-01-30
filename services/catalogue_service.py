import uuid
from database import mysql, Fetch
from services.shopify_graphql import ShopifyGraphQLClient
from encryption import TokenEncryption


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
                raise Exception("Unauthorized: You do not own this store")
            
            # STEP 2: Get store with encrypted credentials
            store = Fetch.get_store_by_id(store_id, user_id)
            if not store:
                raise Exception("Store not found or access denied")
            
            # STEP 3: Decrypt token for Shopify API call
            decrypted_token = TokenEncryption.decrypt_token(
                store["shopify_access_token_encrypted"]
            )
            
            # STEP 4: Initialize Shopify client
            shopify_client = ShopifyGraphQLClient(
                shop_name=store["shopify_shop_name"],
                access_token=decrypted_token
            )
            
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
                        "weightUnit": v.get("weight_unit", "KG").upper(),
                        "title": v.get("title")
                    }
                    for v in variants
                ]
            
            # STEP 6: Create on Shopify with variants
            shopify_product = shopify_client.create_product_with_variants(product_input)
            
            # STEP 7: Upload images
            shopify_images = []
            if images:
                for img in images:
                    media = shopify_client.create_product_media(
                        product_id=shopify_product["id"],
                        image_url=img.get("image_url"),
                        alt_text=img.get("alt_text", "Product image")
                    )
                    shopify_images.append({
                        "shopify_media_id": media["id"],
                        "position": img.get("position", len(shopify_images)),
                        "image_url": img.get("image_url"),
                        "alt_text": img.get("alt_text")
                    })
            
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
    def list_catalogues(user_id: str, store_id: int = None, limit: int = 50, offset: int = 0) -> list:
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

            if store_id:
                cursor.execute(
                    """
                    SELECT c.catalogue_id, c.store_id, c.title, c.price,
                           c.vendor, c.status, c.user_id, c.created_at,
                           m.shopify_product_id,
                           COUNT(DISTINCT cv.variant_id) as variant_count,
                           COUNT(DISTINCT ci.image_id) as image_count
                    FROM catalogue c
                    LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                    LEFT JOIN catalogue_variants cv ON c.catalogue_id = cv.catalogue_id
                    LEFT JOIN catalogue_images ci ON c.catalogue_id = ci.catalogue_id
                    WHERE c.user_id = %s AND c.store_id = %s
                    GROUP BY c.catalogue_id
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (user_id, store_id, limit, offset)
                )
            else:
                cursor.execute(
                    """
                    SELECT c.catalogue_id, c.store_id, c.title, c.price,
                           c.vendor, c.status, c.user_id, c.created_at,
                           m.shopify_product_id,
                           COUNT(DISTINCT cv.variant_id) as variant_count,
                           COUNT(DISTINCT ci.image_id) as image_count
                    FROM catalogue c
                    LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                    LEFT JOIN catalogue_variants cv ON c.catalogue_id = cv.catalogue_id
                    LEFT JOIN catalogue_images ci ON c.catalogue_id = ci.catalogue_id
                    WHERE c.user_id = %s
                    GROUP BY c.catalogue_id
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (user_id, limit, offset)
                )

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
