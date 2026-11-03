import os
import secrets
from urllib.parse import urlencode
import aiohttp
from dotenv import load_dotenv
from quart import url_for
import redis_cache
from utils.encryption import TokenEncryption
from ..stores import mariadb as storedb
from .mariadb import mariadb

load_dotenv()

FLIPKART_AUTHORIZE_URL = "https://api.flipkart.net/oauth-service/oauth/authorize"
FLIPKART_TOKEN_URL = "https://api.flipkart.net/oauth-service/oauth/token"






def generate_state():
    """
    Generate a random state value for OAuth validation.
    """
    return secrets.token_urlsafe(32)


def get_flipkart_config():
    """
    Read and validate Flipkart OAuth configuration.
    """
    app_id = os.environ.get("FLIPKART_APP_ID")
    app_secret = os.environ.get("FLIPKART_APP_SECRET")
    redirect_uri = os.environ.get("APP_DOMAIN") + url_for("platforms.flipkart.flipkart_auth.flipkart_callback")

    if not app_id or not app_secret or not redirect_uri:
        raise ValueError(
            "Missing Flipkart OAuth configuration. "
            "Set FLIPKART_APP_ID, FLIPKART_APP_SECRET, "
            "and FLIPKART_REDIRECT_URI."
        )

    return app_id, app_secret, redirect_uri


def build_flipkart_authorization_url(state: str) -> str:
    """
    Build the URL to which the seller is redirected to authorize Hooter.
    """
    app_id, _, redirect_uri = get_flipkart_config()

    params = f"""client_id={app_id}&redirect_uri={redirect_uri}&response_type=code&scope=Seller_Api&state={state}"""

    return f"{FLIPKART_AUTHORIZE_URL}?{params}"


async def exchange_authorization_code(code: str, state: str) -> dict:
    """
    Exchange the authorization code returned by Flipkart for tokens.
    """
    app_id, app_secret, redirect_uri = get_flipkart_config()

    params = {
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
        "state": state,
        "code": code,
    }

    async with aiohttp.ClientSession() as http_session:
        async with http_session.get(
            FLIPKART_TOKEN_URL,
            params=params,
            auth=aiohttp.BasicAuth(app_id, app_secret),
        ) as response:
            response_data = await response.json(content_type=None)

            if response.status != 200:
                raise RuntimeError(f"Token exchange failed: HTTP {response.status}")

            if not isinstance(response_data, dict):
                raise RuntimeError("Flipkart returned an invalid token response")

            return response_data


async def exchange_refresh_token(refresh_token: str) -> dict:
    """
    Use a refresh token to request a new access token.
    Returns the complete token response.
    """
    app_id, app_secret, _ = get_flipkart_config()

    if not refresh_token:
        raise ValueError("Flipkart refresh token is required")

    params = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    async with aiohttp.ClientSession() as http_session:
        async with http_session.get(
            FLIPKART_TOKEN_URL,
            params=params,
            auth=aiohttp.BasicAuth(app_id, app_secret),
        ) as response:
            response_data = await response.json(content_type=None)

            if response.status != 200:
                raise RuntimeError(f"Token refresh failed: HTTP {response.status}")

            if not isinstance(response_data, dict):
                raise RuntimeError("Flipkart returned an invalid token response")

            if not response_data.get("access_token"):
                raise RuntimeError("Flipkart token response is missing access_token")

            return response_data




async def get_access_token(brand_id: str) -> str:
    """
    Return a valid Flipkart access token for a brand.
    Refresh the token when it is missing from Redis.
    """
    cache_key = f"flipkart:{brand_id}:access_token"

    # Check Redis for the existing access token.
    cached_token = await redis_cache.get(cache_key)
    if cached_token:
        return TokenEncryption.decrypt_token(cached_token)

    
    account = await storedb.Fetch.get_account(brand_id)
    if not account:
        raise ValueError("No Flipkart account is connected to this brand")
    
    # Fetch.get_account() should decrypt the refresh token.
    refresh_token = account["refresh_token"]
    if not refresh_token:
        raise ValueError("Flipkart refresh token is missing")

    # Request a new access token from Flipkart.
    token_data = await exchange_refresh_token(refresh_token)
    new_access_token = token_data.get("access_token")
    expires_in = token_data.get("expires_in")
    if not new_access_token or expires_in is None:
        raise RuntimeError("Flipkart returned an incomplete token response")
    
    try:
        expires_in = int(expires_in)
        if expires_in <= 0:
            raise ValueError
    except (TypeError, ValueError):
        raise RuntimeError("Flipkart returned an invalid access token expiry")

    # Update the refresh token in MariaDB if Flipkart returns a replacement.
    new_refresh_token = token_data.get("refresh_token")
    new_refresh_token_expires_in = token_data.get("refresh_token_expires_in")
    if new_refresh_token:
        result = await mariadb.Write.update_tokens(
            brand_id=brand_id,
            refresh_token=new_refresh_token,
            refresh_token_expires_in=new_refresh_token_expires_in,
        )
        if result.get("status") != "ok":
            raise RuntimeError("Unable to save the refreshed Flipkart refresh token")
        
    # Encrypt the access token and cache it in Redis.
    encrypted_access_token = TokenEncryption.encrypt_token(new_access_token)
    await redis_cache.set(
        cache_key,
        encrypted_access_token,
        expires_in=expires_in,
    )
    
    return new_access_token