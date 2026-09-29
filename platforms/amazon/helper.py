import os 
import secrets
from urllib.parse import urlencode
import aiohttp


def generate_state():
    return secrets.token_urlsafe(32)


def build_amazon_authorization_url(state):
    params = {
        "application_id": os.environ.get("AMAZON_APPLICATION_ID"),
        "state": state,
        "version": "beta"
    }
    return ("https://sellercentral.amazon.com/apps/authorize/consent?" + urlencode(params))


async def exchange_authorization_code(code):
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

async def get_access_token(refresh_token):
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
                raise Exception(f"Amazon access token refresh failed: {data}")
            return data["access_token"]
