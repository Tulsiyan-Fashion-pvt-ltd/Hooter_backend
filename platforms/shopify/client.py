import os
from dotenv import load_dotenv
import aiohttp
from .stores import mariadb
from .auth.services import get_access_token

load_dotenv()

class ShopifyGraphQLClient:
    """SHOPIFY CLIENT FOR THE SHOPIFY QUERY"""
    def __init__(self, store_id:int):
        self.store_id = store_id
        self.shop_name = None
        self.access_token = None
        self.endpoint = f"https://{self.shop_name}/admin/api/{os.getenv('SHOPIFY_API_VERSION')}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token
        }


    async def query(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query against the Shopify API."""

        await self.store_credentials()
        async with aiohttp.ClientSession() as session:
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
            async with session.post(self.endpoint, json=payload, headers=self.headers) as response:
                response.raise_for_status()
                return await response.json()


    async def store_credentials(self):
        self.shop_name = await mariadb.Fetch.shope_name(self.store_id)
        self.access_token = await get_access_token()
        return