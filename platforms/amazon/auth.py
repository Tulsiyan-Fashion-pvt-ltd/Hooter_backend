from quart import request, jsonify, session, abort, redirect
from dotenv import load_dotenv
from urllib.parse import urlencode
import os
from utils.prerequirements import login_required, brand_required
from . import amazon
from . import mariadb
from .helper import generate_state, build_amazon_authorization_url, exchange_authorization_code

load_dotenv()


@amazon.get("/amazon/authorize")
@login_required
@brand_required
async def authorize_amazon():
    state = generate_state()
    session["state"] = state
    authorization_url = build_amazon_authorization_url(state)

    return redirect(authorization_url)


@amazon.get("/amazon/login")
async def amazon_login():
    amazon_callback_uri = request.args.get("amazon_callback_uri")
    amazon_state = request.args.get("amazon_state")
    selling_partner_id = request.args.get("selling_partner_id")

    if not amazon_callback_uri or not amazon_state:
        return jsonify({"status": "error", "message": "Missing Amazon authorization parameters"}), 400

    session["amazon_callback_uri"] = amazon_callback_uri
    session["amazon_state"] = amazon_state
    session["selling_partner_id"] = selling_partner_id

    state = generate_state()
    session["amazon_login_state"] = state

    params = {
        "redirect_uri": os.environ.get("AMAZON_REDIRECT_URI"),
        "amazon_state": amazon_state,
        "state": state
    }

    return redirect(f"{amazon_callback_uri}?{urlencode(params)}")


@amazon.get("/amazon/callback")
@login_required
@brand_required
async def amazon_callback():
    state = request.args.get("state")
    selling_partner_id = request.args.get("selling_partner_id")
    spapi_oauth_code = request.args.get("spapi_oauth_code")

    # Validate state
    if not state or state != session.get("amazon_login_state"):
         return jsonify({"status": "error", "message": "Invalid or missing Amazon state"}), 403

    if not selling_partner_id or not spapi_oauth_code:
        return jsonify({"status": "error", "message": "Missing selling partner ID or authorization code"}), 400

    # Exchange authorization code for tokens
    token_data = await exchange_authorization_code(spapi_oauth_code)

    refresh_token = token_data.get("refresh_token")
    if not refresh_token:
        return jsonify({"status": "error", "message": "Amazon did not return refresh token"}), 400

    # Store encrypted refresh token in database
    result = await mariadb.Write.add_store(
        brand_id=session.get("brand"),
        selling_partner_id=selling_partner_id,
        refresh_token=refresh_token
    )

    if result["status"] == "error":
        return jsonify(result), 400
    
    return jsonify({
        "status": "success",
        "message": "Amazon account connected successfully",
        "selling_partner_id": selling_partner_id,
    }), 200
