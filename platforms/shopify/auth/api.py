"""
    SHOPIFY OAUTH INTEGRATION DOCUMENT
    https://shopify.dev/docs/apps/build/authentication-authorization/authenticate-standalone-apps?lang=node
"""

from quart import Blueprint, request, jsonify, session, abort, redirect
from .utils import verify_hmac
from utils.prerequirements import login_required, brand_required
import os
from dotenv import load_dotenv
from . import services
from security_extensions import validate_csrf
import re
from . import services
from quart_rate_limiter import rate_limit
from datetime import timedelta


load_dotenv()

auth = Blueprint("shopify_auth", __name__, url_prefix = "/auth")




@auth.post("/install-store")
@rate_limit(2, timedelta(seconds=1))
@validate_csrf
@login_required
@brand_required
async def install_store():
    payload = await request.get_json()
    
    if not payload or not verify_hmac(payload):
        return abort(400)

    # checking shop name 
    pattern = r"^[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com$"
    if not re.match(pattern, payload.get("shop")):
        return abort (400)

    session["shopify_shop_name"] = payload.get("shop")
    auth_redirect_url = services.auth_redirect(session.get('shopify_shop_name'))
    return jsonify({"status": "acquired the shop name", "redirect": auth_redirect_url}), 200




@auth.get("/callback")
@login_required
@brand_required
async def auth_callback():
    params = request.args.to_dict()
    params.pop("hmac", None)

    if not params or verify_hmac(params) or params.get("state") != session.get("shopify_state"):
        return abort(403)
    
    service_res = await services.exchange_token(session.get('brand'), params.get('code'), session.get('shopify_shop_name'))
    if service_res.get('status') == "failed":
        if service_res.get('code') == '4a5304a1':
            return service_res.get('error')
            
        return service_res
    
    session.pop('shopify_shop_name', None)
    return redirect(os.environ.get("DASHBOARD_DOMAIN"))