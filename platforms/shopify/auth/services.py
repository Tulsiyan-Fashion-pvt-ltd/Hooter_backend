from quart import session, abort
import aiohttp
from ..stores import mariadb
import os
from dotenv import load_dotenv
from typing import Literal
import secrets
from urllib.parse import urlencode

load_dotenv()




async def exchange_token(code) -> dict[str, str]:
    """Exchange `code` for `refresh_token` and `access_token`.
    This function is only meant for Authentication not acquiring `access_token`
    
    Returns:
        dict:
            - On success:
                `status`: `Literal['successful']`
            - On failure:
                `status` : `Literal['failed']`
    """
    '''making the post request to exchange the access token'''

    url = f"https://{session.get("shopify_shop_name")}/admin/oauth/access_token"
    payload = {
        "client_id": os.environ.get("SHOPIFY_CLIENT_ID"),
        "client_secret": os.environ.get("SHOPIFY_CLIENT_SECRET"),
        "code": code,
        "expiring": '1'
    }

    access_token = None
    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(url, json=payload) as response:
            if response.status != 200:
                raise Exception(await response.text())

            data = await response.json()
            access_token = data.get('access_token')     # store that access token in server cache
            refresh_token = data.get("refresh_token")
            refresh_token_expires_in = data.get('refresh_token_expires_in')

    '''save this access token in the db'''
    sql_response = await mariadb.Write.add_store(session.get("brand"), session.get("shopify_shop_name"), refresh_token, refresh_token_expires_in)
    if sql_response.get('status') == 'error':
        return {'status': 'failed', 'message': sql_response.get('error')}
    else:
        return sql_response



def auth_redirect():
    shop = session.get("shopify_shop_name")
    session["shopify_state"] = secrets.token_urlsafe(32)
    params = {
        "client_id": os.environ.get("SHOPIFY_CLIENT_ID"),
        "scope": "read_orders,write_orders, read_products,write_products,read_customers,write_customers,read_inventory,write_inventory",
        "redirect_uri": f"{os.environ.get("APP_DOMAIN")}/platforms/shopify/auth/callback",
        "state": session.get("shopify_state")
    }

    url = f"https://{shop}/admin/oauth/authorize?{urlencode(params)}"
    return url