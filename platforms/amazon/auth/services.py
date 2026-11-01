import os 
import secrets
from urllib.parse import urlencode
import aiohttp
from utils.encryption import TokenEncryption
from ..stores.mariadb import Fetch
import redis_cache


def generate_state():
    return secrets.token_urlsafe(32)


def build_amazon_authorization_url(state):
    params = {
        "application_id": os.environ.get("AMAZON_APPLICATION_ID"),
        "state": state,
        "version": "beta"
    }
    return ("https://sellercentral.amazon.com/apps/authorize/consent?" + urlencode(params))


async def exchange_token(code):
    url = "https://api.amazon.com/auth/o2/token"

    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": os.environ.get("AMAZON_REDIRECT_URI"),
        "client_id": os.environ.get("AMAZON_CLIENT_ID"),
        "client_secret": os.environ.get("AMAZON_CLIENT_SECRET"),
    }

    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(url, data=payload) as response:
            data = await response.json()
            if response.status != 200:
                raise Exception(f"Amazon token exchange failed: {data}")
            return data


async def get_access_token(brand_id: str, selling_partner_id: str) -> str:
    """Get a cached Amazon access token or refresh it using MariaDB credentials."""

    store = await Fetch.get_store(brand_id, selling_partner_id)
    if not store:
        raise RuntimeError("Amazon store connection not found")

    store_id = store["store_id"]
    redis_key = f"amazon:{store_id}:access_token"

    # 1. Check Redis first
    cached_token = await redis_cache.get(redis_key)
    if cached_token:
        access_token = TokenEncryption.decrypt_token(cached_token)
        if access_token:
            return access_token

    # 2. Get and decrypt refresh token from MariaDB
    refresh_token = TokenEncryption.decrypt_token(
        store["amazon_refresh_token_encrypted"]
    )

    if not refresh_token:
        raise RuntimeError("Amazon refresh token not found")

    # 3. Request a new access token from Amazon LWA
    url = "https://api.amazon.com/auth/o2/token"

    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": os.environ.get("AMAZON_CLIENT_ID"),
        "client_secret": os.environ.get("AMAZON_CLIENT_SECRET"),
    }

    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(url, data=payload) as response:
            data = await response.json()
            if response.status != 200:
                raise RuntimeError(f"Amazon access token refresh failed: {data}")
            
    access_token = data["access_token"]
    expires_in = data["expires_in"]

    # 4. Encrypt and cache the access token in Redis
    encrypted_token = TokenEncryption.encrypt_token(access_token)
    redis_result = await redis_cache.set(
        redis_key,
        encrypted_token,
        expires_in=expires_in
    )

    if redis_result is False:
        raise RuntimeError("Could not store Amazon access token in Redis")

    return access_token

