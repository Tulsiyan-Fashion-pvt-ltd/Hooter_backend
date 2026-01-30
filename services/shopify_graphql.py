import requests
import time


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
            raise Exception(data["data"]["productCreate"]["userErrors"])
        
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
            raise Exception(data["data"]["productCreateMedia"]["userErrors"])
        
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
            raise Exception(user_errors)

        return data["data"]["productCreate"]["product"]

