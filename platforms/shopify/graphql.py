import os
from dotenv import load_dotenv
import aiohttp

load_dotenv()

class ShopifyGraphQLClient:
    def __init__(self, shop_name: str, access_token: str):
        self.shop_name = shop_name
        self.access_token = access_token
        self.endpoint = f"https://{shop_name}/admin/api/{os.getenv('SHOPIFY_API_VERSION')}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": access_token
        }

    async def query(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query against the Shopify API."""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            async with session.post(self.endpoint, json=payload, headers=self.headers) as response:
                response.raise_for_status()
                return await response.json()