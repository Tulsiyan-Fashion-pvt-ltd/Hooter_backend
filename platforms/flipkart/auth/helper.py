import os
import secrets
from urllib.parse import urlencode
import aiohttp
from dotenv import load_dotenv

load_dotenv()

FLIPKART_AUTHORIZE_URL = ("https://api.flipkart.net/oauth-service/oauth/authorize")
FLIPKART_TOKEN_URL = ("https://api.flipkart.net/oauth-service/oauth/token")


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
    redirect_uri = os.environ.get("FLIPKART_REDIRECT_URI")

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

    params = {
        "app_id": app_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "Seller_Api",
        "state": state,
    }

    return f"{FLIPKART_AUTHORIZE_URL}?{urlencode(params)}"


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


async def get_access_token(refresh_token: str) -> dict:
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