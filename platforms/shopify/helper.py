import logging
from utils.encryption import TokenEncryption
from .mariadb import Fetch
from shopify_archives.graphql import ShopifyGraphQLClient
from shopify_archives.exceptions import AuthorizationError, ShopifyAPIError
import hmac
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


async def get_store_config(store_id: int, user_id: str) -> dict:
    """Fetch store + decrypt token for Shopify client usage."""
    store = await Fetch.get_store_by_id(store_id, user_id)
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
    query validateShop {
      shop { name }
    }
    """
    try:
        import requests
        resp = requests.post(
            client.endpoint,
            json={"query": query},
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
    

def verify_hmac(args):
    # Copy parameters
    params = dict(args)
    SHOPIFY_CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")

    # Remove hmac
    received_hmac = params.pop("hmac", None)

    if not received_hmac:
        return False

    # Sort alphabetically
    sorted_params = sorted(params.items())

    # Build message
    message = "&".join(
        f"{key}={value}"
        for key, value in sorted_params
    )

    # Generate HMAC
    generated_hmac = hmac.new(
        SHOPIFY_CLIENT_SECRET.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(
        generated_hmac,
        received_hmac
    )