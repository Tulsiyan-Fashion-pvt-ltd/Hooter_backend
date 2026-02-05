import logging
import requests
import time

from services.exceptions import ShopifyAPIError


class ShopifyRetryableError(Exception):
    """Retryable Shopify API error for transient failures."""


logger = logging.getLogger(__name__)



class ShopifyGraphQLClient:
    def __init__(self, shop_name: str, access_token: str, api_version: str = "2024-01"):
        self.endpoint = (
            f"https://{shop_name}.myshopify.com/"
            f"admin/api/{api_version}/graphql.json"
        )
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": access_token
        }

    @staticmethod
    def handle_rate_limit(response_data: dict) -> bool:
        """
        Check Shopify API response for rate limit throttling.
        
        Shopify uses cost-based throttle. If API call cost exceeds budget,
        we need to backoff and retry.
        
        Args:
            response_data: Parsed JSON response from Shopify GraphQL API
        
        Returns:
            True if rate limited and backed off, False otherwise
        """
        try:
            extensions = response_data.get("extensions", {})
            cost = extensions.get("cost", {})
            throttle = cost.get("throttleStatus", {})
            
            # Check if currently available capacity is low
            currently_available = throttle.get("currentlyAvailable", 1000)
            max_available = throttle.get("maximumAvailable", 1000)
            
            # If less than 10% capacity available, back off
            if currently_available < (max_available * 0.1):
                rest_time_ms = throttle.get("restoreRate", 50)
                # Convert ms to seconds, add buffer
                sleep_time = (rest_time_ms / 1000) + 1
                print(f"[Shopify Rate Limit] Available: {currently_available}/{max_available}")
                print(f"[Shopify Rate Limit] Backing off for {sleep_time}s")
                time.sleep(sleep_time)
                return True
        except Exception as e:
            # If we can't parse throttle info, continue anyway
            print(f"Warning: Could not parse rate limit info: {str(e)}")
        
        return False

    def create_product_with_variants(self, product_input: dict) -> dict:
        """
        Create product with variants in single mutation.
        
        Args:
            product_input: Dict with title, variants, vendor, etc.
        
        Returns:
            Dict with product ID, title, and variants
        """
        query = """
        mutation productCreate($input: ProductInput!) {
          productCreate(input: $input) {
            product {
              id
              title
              variants(first: 10) {
                edges {
                  node {
                    id
                    sku
                    price
                    inventoryItem { id }
                  }
                }
              }
            }
            userErrors { field, message }
          }
        }
        """
        
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": {"input": product_input}},
            headers=self.headers,
            timeout=15
        )
        
        response.raise_for_status()
        data = response.json()
        
        # Check for rate limiting
        ShopifyGraphQLClient.handle_rate_limit(data)
        
        if data["data"]["productCreate"]["userErrors"]:
            errors = data["data"]["productCreate"]["userErrors"]
            logger.error("Shopify productCreate errors: %s", errors)
            raise ShopifyAPIError(errors)
        
        product = data["data"]["productCreate"]["product"]
        
        # Flatten variants from edges
        variants = [
            {
                "id": edge["node"]["id"],
                "sku": edge["node"]["sku"],
                "price": edge["node"]["price"],
                "inventoryItem": edge["node"]["inventoryItem"]
            }
            for edge in product["variants"]["edges"]
        ]
        
        return {
            "id": product["id"],
            "title": product["title"],
            "variants": variants
        }

    def create_product_media(self, product_id: str, image_url: str, alt_text: str) -> dict:
        """
        Upload image to Shopify product.
        
        Args:
            product_id: Shopify product ID (gid://shopify/Product/xxx)
            image_url: URL to image (Google Drive link or other)
            alt_text: Alternative text for accessibility
        
        Returns:
            Dict with media ID
        """
        query = """
        mutation createProductMedia($input: CreateMediaInput!) {
          productCreateMedia(input: $input) {
            media {
              id
              alt
            }
            userErrors { field, message }
          }
        }
        """
        
        # Shopify expects originalSource as a direct URL string, not an object
        variables = {
            "input": {
                "productId": product_id,
                "media": [
                    {
                        "mediaContentType": "IMAGE",
                        "originalSource": image_url  # Direct URL string, not {"url": ...}
                    }
                ],
                "alt": {
                    "value": alt_text
                }
            }
        }
        
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        
        response.raise_for_status()
        data = response.json()
        
        # Check for rate limiting
        ShopifyGraphQLClient.handle_rate_limit(data)
        
        if data["data"]["productCreateMedia"]["userErrors"]:
            errors = data["data"]["productCreateMedia"]["userErrors"]
            logger.error("Shopify productCreateMedia errors: %s", errors)
            raise ShopifyAPIError(errors)
        
        return {
            "id": data["data"]["productCreateMedia"]["media"]["id"]
        }

    def create_product(self, title: str, description: str, price: str) -> dict:
        """
        Simple product creation (legacy - for backwards compatibility).
        """
        query = """
        mutation productCreate($input: ProductInput!) {
          productCreate(input: $input) {
            product {
              id
              title
            }
            userErrors {
              field
              message
            }
          }
        }
        """

        variables = {
            "input": {
                "title": title,
                "descriptionHtml": description,
                "variants": [
                    {
                        "price": price
                    }
                ]
            }
        }

        response = requests.post(
            self.endpoint,
            json={
                "query": query,
                "variables": variables
            },
            headers=self.headers,
            timeout=15
        )

        response.raise_for_status()
        data = response.json()

        user_errors = data["data"]["productCreate"]["userErrors"]
        if user_errors:
            logger.error("Shopify legacy productCreate errors: %s", user_errors)
            raise ShopifyAPIError(user_errors)

        return data["data"]["productCreate"]["product"]

    def update_product(self, product_id: str, product_input: dict) -> dict:
        """Update a Shopify product."""
        query = """
        mutation productUpdate($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id title }
            userErrors { field message }
          }
        }
        """

        variables = {"input": {"id": product_id, **product_input}}
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["productUpdate"]["userErrors"]
        if errors:
            logger.error("Shopify productUpdate errors: %s", errors)
            raise ShopifyAPIError(errors)
        return data["data"]["productUpdate"]["product"]

    def delete_product(self, product_id: str) -> dict:
        """Delete a Shopify product (hard delete)."""
        query = """
        mutation productDelete($input: ProductDeleteInput!) {
          productDelete(input: $input) {
            deletedProductId
            userErrors { field message }
          }
        }
        """
        variables = {"input": {"id": product_id}}
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["productDelete"]["userErrors"]
        if errors:
            logger.error("Shopify productDelete errors: %s", errors)
            raise ShopifyAPIError(errors)
        return {"deletedProductId": data["data"]["productDelete"]["deletedProductId"]}

    def reorder_product_media(self, product_id: str, media_ids: list) -> list:
        """Reorder product media according to provided media IDs."""
        query = """
        mutation productReorderMedia($productId: ID!, $mediaIds: [ID!]!) {
          productReorderMedia(productId: $productId, mediaIds: $mediaIds) {
            product {
              media(first: 100) {
                edges { node { id } }
              }
            }
            userErrors { field message }
          }
        }
        """
        variables = {"productId": product_id, "mediaIds": media_ids}
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["productReorderMedia"]["userErrors"]
        if errors:
            logger.error("Shopify productReorderMedia errors: %s", errors)
            raise ShopifyAPIError(errors)
        return [edge["node"]["id"] for edge in data["data"]["productReorderMedia"]["product"]["media"]["edges"]]

    def get_locations(self) -> list:
        """Fetch Shopify locations for inventory sync."""
        query = """
        query locations($first: Int!) {
          locations(first: $first) {
            edges {
              node { id name }
            }
          }
        }
        """
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": {"first": 50}},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        edges = data["data"]["locations"]["edges"]
        return [{"id": edge["node"]["id"], "name": edge["node"]["name"]} for edge in edges]

    def activate_inventory(self, inventory_item_id: str, location_id: str) -> dict:
        """Activate inventory at a location for an inventory item."""
        query = """
        mutation inventoryActivate($inventoryItemId: ID!, $locationId: ID!) {
          inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId) {
            inventoryLevel { id }
            userErrors { field message }
          }
        }
        """
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": {"inventoryItemId": inventory_item_id, "locationId": location_id}},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["inventoryActivate"]["userErrors"]
        if errors:
            logger.error("Shopify inventoryActivate errors: %s", errors)
            raise ShopifyAPIError(errors)
        return {"inventoryLevelId": data["data"]["inventoryActivate"]["inventoryLevel"]["id"]}

    def set_inventory_quantities(self, inventory_item_id: str, location_id: str, available: int) -> dict:
        """Set inventory on hand quantities."""
        query = """
        mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
          inventorySetOnHandQuantities(input: $input) {
            inventoryLevels {
              id
              available
            }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": {
                "setQuantities": [
                    {
                        "inventoryItemId": inventory_item_id,
                        "locationId": location_id,
                        "availableQuantity": int(available)
                    }
                ]
            }
        }
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["inventorySetOnHandQuantities"]["userErrors"]
        if errors:
            logger.error("Shopify inventorySetOnHandQuantities errors: %s", errors)
            raise ShopifyAPIError(errors)
        return data["data"]["inventorySetOnHandQuantities"]["inventoryLevels"][0]

    def update_variant(self, variant_id: str, variant_input: dict) -> dict:
        """Update a product variant."""
        query = """
        mutation productVariantUpdate($input: ProductVariantInput!) {
          productVariantUpdate(input: $input) {
            productVariant { id sku price }
            userErrors { field message }
          }
        }
        """
        variables = {"input": {"id": variant_id, **variant_input}}
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["productVariantUpdate"]["userErrors"]
        if errors:
            logger.error("Shopify productVariantUpdate errors: %s", errors)
            raise ShopifyAPIError(errors)
        return data["data"]["productVariantUpdate"]["productVariant"]

    def create_variant(self, product_id: str, variant_input: dict) -> dict:
        """Create a new variant for a product."""
        query = """
        mutation productVariantCreate($input: ProductVariantInput!) {
          productVariantCreate(input: $input) {
            productVariant { id sku price inventoryItem { id } }
            userErrors { field message }
          }
        }
        """
        variables = {"input": {"productId": product_id, **variant_input}}
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["productVariantCreate"]["userErrors"]
        if errors:
            logger.error("Shopify productVariantCreate errors: %s", errors)
            raise ShopifyAPIError(errors)
        return data["data"]["productVariantCreate"]["productVariant"]

    def delete_variant(self, variant_id: str) -> dict:
        """Delete a variant by ID."""
        query = """
        mutation productVariantDelete($id: ID!) {
          productVariantDelete(id: $id) {
            deletedProductVariantId
            userErrors { field message }
          }
        }
        """
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": {"id": variant_id}},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        errors = data["data"]["productVariantDelete"]["userErrors"]
        if errors:
            logger.error("Shopify productVariantDelete errors: %s", errors)
            raise ShopifyAPIError(errors)
        return {"deletedProductVariantId": data["data"]["productVariantDelete"]["deletedProductVariantId"]}

    def list_product_media(self, product_id: str) -> list:
        """List product media IDs (used for reordering)."""
        query = """
        query productMedia($id: ID!) {
          product(id: $id) {
            media(first: 100) {
              edges { node { id } }
            }
          }
        }
        """
        response = requests.post(
            self.endpoint,
            json={"query": query, "variables": {"id": product_id}},
            headers=self.headers,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        ShopifyGraphQLClient.handle_rate_limit(data)
        edges = data["data"]["product"]["media"]["edges"]
        return [edge["node"]["id"] for edge in edges]



