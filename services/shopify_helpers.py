import logging
from encryption import TokenEncryption
from database import Fetch
from services.shopify_graphql import ShopifyGraphQLClient
from services.exceptions import AuthorizationError, ShopifyAPIError


logger = logging.getLogger(__name__)


def get_store_config(store_id: int, user_id: str) -> dict:
    """Fetch store + decrypt token for Shopify client usage."""
    store = Fetch.get_store_by_id(store_id, user_id)
    if not store:
        raise AuthorizationError("Store not found or access denied")
    token = TokenEncryption.decrypt_token(store["shopify_access_token_encrypted"])
    return {
        "store": store,
        "shop_name": store["shopify_shop_name"],
        "token": token,
        "client": ShopifyGraphQLClient(store["shopify_shop_name"], token)
    }


def validate_shopify_token(shop_name: str, access_token: str) -> None:
    """Validate Shopify access token with a harmless query."""
    client = ShopifyGraphQLClient(shop_name, access_token)
    query = """
    query validateShop($first: Int!) {
      shop { name }
    }
    """
    try:
        import requests
        resp = requests.post(
            client.endpoint,
            json={"query": query, "variables": {"first": 1}},
            headers=client.headers,
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        user_errors = data.get("errors") or []
        if user_errors:
            logger.error("Shopify token validation errors for %s: %s", shop_name, user_errors)
            raise ShopifyAPIError("Invalid Shopify token")
    except Exception as exc:
        logger.error("Shopify token validation failed for %s: %s", shop_name, str(exc))
        raise ShopifyAPIError("Invalid Shopify token")