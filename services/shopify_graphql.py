import logging
import requests
import time

from services.exceptions import ShopifyAPIError


class ShopifyRetryableError(Exception):
    """Retryable Shopify API error for transient failures."""


logger = logging.getLogger(__name__)



class ShopifyGraphQLClient:
    def __init__(self, shop_name: str, access_token: str, api_version: str = "2023-04"):
        self.shop_name = shop_name
        self.access_token = access_token
        self.api_version = api_version
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
        Create product with variants using REST API.

        Args:
            product_input: Dict with title, variants, vendor, etc.

        Returns:
            Dict with product ID, title, and variants
        """
        # Build REST API payload
        product_data = {
            "product": {
                "title": product_input.get("title"),
                "body_html": product_input.get("descriptionHtml"),
                "vendor": product_input.get("vendor"),
                "product_type": product_input.get("productType"),
                "tags": ",".join(product_input.get("tags", [])),
                "variants": []
            }
        }

        # Add variants
        variants = product_input.get("variants", [])
        for variant in variants:
            product_data["product"]["variants"].append({
                "sku": variant.get("sku"),
                "price": variant.get("price"),
                "compare_at_price": variant.get("compareAtPrice"),
                "weight": variant.get("weight"),
                "weight_unit": variant.get("weightUnit", "kg").lower(),
                "title": variant.get("title")
            })

        # REST API endpoint
        rest_endpoint = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}/products.json"

        response = requests.post(
            rest_endpoint,
            json=product_data,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": self.access_token
            },
            timeout=30
        )

        response.raise_for_status()
        data = response.json()

        product = data["product"]
        created_variants = [
            {
                "id": f"gid://shopify/ProductVariant/{v['id']}",
                "sku": v.get("sku"),
                "price": str(v.get("price", 0)),
                "inventoryItem": {"id": f"gid://shopify/InventoryItem/{v.get('inventory_item_id')}"}
            }
            for v in product["variants"]
        ]

        return {
            "id": f"gid://shopify/Product/{product['id']}",
            "title": product["title"],
            "variants": created_variants
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
        """Delete a Shopify product (hard delete) using REST API."""
        # Extract numeric ID from gid if needed
        if product_id.startswith("gid://shopify/Product/"):
            product_id = product_id.split("/")[-1]

        rest_endpoint = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}/products/{product_id}.json"

        response = requests.delete(
            rest_endpoint,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": self.access_token
            },
            timeout=15
        )

        response.raise_for_status()
        # REST delete returns empty body on success
        return {"deletedProductId": product_id}

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

        # Check for GraphQL errors
        if "errors" in data:
            logger.error("Shopify GraphQL errors: %s", data["errors"])
            raise ShopifyAPIError(data["errors"])

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



