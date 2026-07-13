from quart import Blueprint, request, jsonify, session, abort, redirect
import requests
from . import mariadb
from .helper import validate_shopify_token, ShopifyAPIError, verify_hmac
from utils.prerequirements import login_required, brand_required
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
import secrets
import aiohttp
from . import shopify

load_dotenv()


"""
SHOPIFY OAUTH INTEGRATION DOCUMENT
https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/authorization-code-grant  
"""

@shopify.post("/shopify/install-store")
@login_required
@brand_required
async def install_store():
    payload = await request.get_json()
    
    if not payload or not verify_hmac(payload):
        return abort(400)
    
    session["shopify_shop_name"] = payload.get("shop")
    auth_redirect_url = auth()
    return jsonify({"status": "acquired the shop name", "redirect": auth_redirect_url}), 200


def auth():
    shop = session.get("shopify_shop_name")
    session["shopify_state"] = secrets.token_urlsafe(32)
    params = {
        "client_id": os.environ.get("SHOPIFY_CLIENT_ID"),
        "scope": "read_orders,write_orders, read_products,write_products,read_customers,write_customers,read_inventory,write_inventory",
        "redirect_uri": f"{os.environ.get("APP_DOMAIN")}/shopify/auth/callback",
        "state": session.get("shopify_state")
    }

    url = f"https://{shop}/admin/oauth/authorize?{urlencode(params)}"
    return url


@shopify.get("/shopify/auth/callback")
@login_required
@brand_required
async def auth_callback():
    params = request.args.to_dict()
    params.pop("hmac", None)

    if not params or verify_hmac(params) or params.get("state") != session.get("shopify_state"):
        return abort(403)
    
    '''making the post request to exchange the access token'''
    url = f"https://{session.get("shopify_shop_name")}/admin/oauth/access_token"
    payload = {
        "client_id": os.environ.get("SHOPIFY_CLIENT_ID"),
        "client_secret": os.environ.get("SHOPIFY_CLIENT_SECRET"),
        "code": params.get("code"),
        "expiring": 0
    }

    access_token = None
    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(url, json=payload) as response:
            if response.status != 200:
                raise Exception(await response.text())

            data = await response.json()
            access_token = data.get("access_token")

    '''save this access token in the db'''
    await mariadb.Write.add_store(session.get("brand"), session.get("shopify_shop_name"), access_token)
    return redirect(os.environ.get("DASHBOARD_DOMAIN"))