import uuid
from database import mysql
from services.shopify_graphql import ShopifyGraphQLClient


class CatalogueService:
    """Service for managing catalogue operations with Shopify sync."""

    @staticmethod
    def validate_input(data: dict) -> dict:
        """
        Validate catalogue input data.

        Args:
            data: Dictionary containing title, description, price

        Returns:
            dict: Validation errors (empty if valid)

        Raises:
            ValueError: If validation fails
        """
        errors = {}

        if not data.get("title") or not isinstance(data["title"], str):
            errors["title"] = "Title is required and must be a string"

        if not data.get("description") or not isinstance(data["description"], str):
            errors["description"] = "Description is required and must be a string"

        price = data.get("price")
        if price is None:
            errors["price"] = "Price is required"
        else:
            try:
                float(price)
                if float(price) < 0:
                    errors["price"] = "Price must be non-negative"
            except (ValueError, TypeError):
                errors["price"] = "Price must be a valid number"

        if errors:
            raise ValueError(str(errors))

        return data

    @staticmethod
    def create_catalogue(
        title: str,
        description: str,
        price: str,
        shopify_config: dict,
        user_id: str = None
    ) -> dict:
        """
        Create a catalogue entry and sync to Shopify.

        Args:
            title: Product title
            description: Product description
            price: Product price
            shopify_config: Dictionary with shop_name and access_token
            user_id: Optional user ID for tracking

        Returns:
            dict: Catalogue data with local_id and shopify_id

        Raises:
            Exception: If Shopify sync or database operation fails
        """
        cursor = None
        try:
            # Validate input
            CatalogueService.validate_input({
                "title": title,
                "description": description,
                "price": price
            })

            # Initialize Shopify client
            shopify_client = ShopifyGraphQLClient(
                shop_name=shopify_config["shop_name"],
                access_token=shopify_config["access_token"]
            )

            # Create product on Shopify
            shopify_product = shopify_client.create_product(
                title=title,
                description=description,
                price=str(price)
            )

            # Generate local catalogue ID
            catalogue_id = str(uuid.uuid4())
            cursor = mysql.connection.cursor()

            # Begin transaction
            try:
                # Insert catalogue record
                cursor.execute(
                    """
                    INSERT INTO catalogue (
                        catalogue_id, title, description, price, user_id, created_at
                    ) VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (catalogue_id, title, description, price, user_id)
                )

                # Insert Shopify mapping
                cursor.execute(
                    """
                    INSERT INTO catalogue_shopify_mapping (
                        catalogue_id, shopify_product_id, synced_at
                    ) VALUES (%s, %s, NOW())
                    """,
                    (catalogue_id, shopify_product["id"])
                )

                mysql.connection.commit()

                return {
                    "local_id": catalogue_id,
                    "shopify_id": shopify_product["id"],
                    "title": shopify_product["title"],
                    "message": "Catalogue created and synced successfully"
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
    def get_catalogue_by_id(catalogue_id: str) -> dict:
        """
        Retrieve catalogue details by ID.

        Args:
            catalogue_id: UUID of the catalogue

        Returns:
            dict: Catalogue data with Shopify mapping
        """
        cursor = None
        try:
            cursor = mysql.connection.cursor()
            cursor.execute(
                """
                SELECT c.catalogue_id, c.title, c.description, c.price,
                       c.user_id, c.created_at, m.shopify_product_id, m.synced_at
                FROM catalogue c
                LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                WHERE c.catalogue_id = %s
                """,
                (catalogue_id,)
            )

            result = cursor.fetchone()
            if result:
                return {
                    "catalogue_id": result[0],
                    "title": result[1],
                    "description": result[2],
                    "price": result[3],
                    "user_id": result[4],
                    "created_at": result[5],
                    "shopify_product_id": result[6],
                    "synced_at": result[7]
                }
            return None

        finally:
            if cursor:
                cursor.close()

    @staticmethod
    def list_catalogues(user_id: str = None, limit: int = 50, offset: int = 0) -> list:
        """
        Retrieve catalogue list with optional filtering.

        Args:
            user_id: Optional filter by user ID
            limit: Number of records to return
            offset: Pagination offset

        Returns:
            list: Catalogue records
        """
        cursor = None
        try:
            cursor = mysql.connection.cursor()

            if user_id:
                cursor.execute(
                    """
                    SELECT c.catalogue_id, c.title, c.description, c.price,
                           c.user_id, c.created_at, m.shopify_product_id
                    FROM catalogue c
                    LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                    WHERE c.user_id = %s
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (user_id, limit, offset)
                )
            else:
                cursor.execute(
                    """
                    SELECT c.catalogue_id, c.title, c.description, c.price,
                           c.user_id, c.created_at, m.shopify_product_id
                    FROM catalogue c
                    LEFT JOIN catalogue_shopify_mapping m ON c.catalogue_id = m.catalogue_id
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (limit, offset)
                )

            results = cursor.fetchall()
            return [
                {
                    "catalogue_id": row[0],
                    "title": row[1],
                    "description": row[2],
                    "price": row[3],
                    "user_id": row[4],
                    "created_at": row[5],
                    "shopify_product_id": row[6]
                }
                for row in results
            ]

        finally:
            if cursor:
                cursor.close()
