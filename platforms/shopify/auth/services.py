"""
The auth services provided authentication services like `exchange_token`, `get_access_token` etc.

NOTE: CALL `get_access_token()` coroutine function to get the access token for shopify rather then 
accessing through `redis_cache`

The shopify access tokens are encrypted and stored in the redis_cache with keys
`shopify:{store_id}:access_token`
"""

from quart import session, render_template
import aiohttp
from ..stores import mariadb as shopify_store_db
import os
from dotenv import load_dotenv
from typing import Literal
import secrets
from urllib.parse import urlencode
import redis_cache
from brand.auth.authorize import brand_access_required
from .authorize import active_shopify_store_access_required
from utils.encryption import TokenEncryption
from datetime import datetime
from traceback import print_exc

load_dotenv()



def auth_redirect(shopify_shop_name):
    """Making redirection url for seller authorization"""
    shop = shopify_shop_name
    session["shopify_state"] = secrets.token_urlsafe(32)
    params = {
        "client_id": os.environ.get("SHOPIFY_CLIENT_ID"),
        "scope": "read_orders,write_orders, read_products,write_products,read_customers,write_customers,read_inventory,write_inventory",
        "redirect_uri": f"{os.environ.get("APP_DOMAIN")}/platforms/shopify/auth/callback",
        "state": session.get("shopify_state")
    }

    url = f"https://{shop}/admin/oauth/authorize?{urlencode(params)}"
    return url




@brand_access_required
async def exchange_token(brand_id: str, code: str|int, shopify_shop_name) -> dict[str, str]:
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

    url = f"https://{shopify_shop_name}/admin/oauth/access_token"
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
                content_type_header = response.headers.get('Content-Type', '')
                if "text/html" in content_type_header:
                    return {'status': 'failed', 'code': '4a5304a1', 'message': 'Duplicate or invalid shopify callback request', 'error': await response.text()}
                    
                raise {'status': 'failed', 'code': '21a1f09f', 'message': 'Shopify oauth code exchange process failed'}

            data = await response.json()
            access_token = data.get('access_token')     # store that access token in server cache
            expires_in = data.get('expires_in')
            refresh_token = data.get("refresh_token")
            refresh_token_expires_in = data.get('refresh_token_expires_in')


    '''save this access token in the db'''
    sql_response = await shopify_store_db.Write.add_store(brand_id, shopify_shop_name, refresh_token, refresh_token_expires_in)
    if sql_response.get('status') == 'error':
        return {'status': 'failed', 'message': sql_response.get('error'), 'code': '28dfdc68'}
    else:
        '''save access token in redis cache'''
        store_id = sql_response.get('store_id')
        redis_store = await redis_cache.set(f"shopify:{store_id}:access_token", TokenEncryption.encrypt_token(access_token), expires_in=expires_in)
        if redis_store == False:
            return {'status': 'failed', 'message': 'Could not store the access_token', 'code': '15c4fe46'}
        return sql_response



@brand_access_required
@active_shopify_store_access_required
async def get_access_token(store_id: str) -> str:
    """Get access token function for the shopify
    
    Args:
        brand_id: 
            Unique Id for brand
            
    Returns:
        - on success:
            access_token `str`
        - raise Exception
    """
    
    access_token = TokenEncryption.decrypt_token(await redis_cache.get(f'shopify:{store_id}:access_token'))
    if not access_token:
        shopify_creds = await shopify_store_db.Fetch.store_creds(store_id)

        if shopify_creds.get('error') or shopify_creds.get('refresh_token') == None:
            return {"status": "failed", "code": "cb7c80c8", "message": "Could not fetch refresh token from shopify store database"}

        '''checking the refresh_token validation'''
        last_update = shopify_creds.get('updated_at')
        current_date = datetime.now()
        validation_sec = shopify_creds.get('refresh_token_expires_in_seconds')
        validate_till = last_update + validation_sec # datetime timestamp

        if current_date >= validate_till:
            raise {'status': 'failed', 'code': '69490a62','message': 'Shopify refresh token has expired'}      # SEND A NOTIFICATION FOR THIS


        '''refresh token change for access token'''
        async with aiohttp.ClientSession() as http_session:
            url = f"https://${shopify_creds.get('shopify_shop_name')}/admin/oauth/access_token"

            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }

            body={
                "client_id": os.environ.get('SHOPIFY_CLIENT_ID'),
                "client_secret": os.environ.get('SHOPIFY_CLIENT_SECRET'),
                "grant_type": "refresh_token",
                "refresh_token": shopify_creds.get('refresh_token'),
            }

            try:
                async with http_session.post(url, json=body, headers=headers) as response:
                    if response.status != 200:
                        print(await response.json())
                        return {'status': 'failed', 'message': 'Shopify refresh token exchange failed', 'code': '5ee530db'}

                    data = await response.json()
                    access_token = data.get('access_token')
                    expires_in = data.get('expires_in')
                    refresh_token = data.get('refresh_token')
                    refresh_token_expires_in = data.get('refresh_token_expires_in')

            except Exception as e:
                print(e)
                print_exc()
                return {'status': 'failed', 'message': 'Shopify refresh token exchange failed', 'code': 'a0569b58'}

        # UPDATE THE REFRESH TOKEN AND ADD ACCESS TOKEN TO THE DB AND CACHE ACCORDINGLY
        update_response = await shopify_store_db.Write.update_refresh_token(store_id, refresh_token, refresh_token_expires_in)
        if update_response.get('status') == 'error':
            raise {'status': 'failed', 'message': "Could not update the refresh token in the DB"+ update_response.get('message'), 'code': '33c9f310'}

        # write encrypted access token and expire in cache
        encrypted_access_token = TokenEncryption.encrypt_token(access_token)
        redis_set_response = await redis_cache.set(f'shopify:{store_id}:access_token', encrypted_access_token, expires_in=expires_in)
        if redis_set_response == False:
            raise {'status': 'failed', 'message': 'Could not store the refreshed access token', 'code': '7f840fd8'}

    return access_token