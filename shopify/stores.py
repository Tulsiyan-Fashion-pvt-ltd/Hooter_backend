from quart import Blueprint, request, jsonify, session, abort, redirect
import requests
from sql_queries import shopify_storesdb
from services.shopify_helpers import validate_shopify_token, ShopifyAPIError, verify_hmac
from utils.prerequirements import login_required, brand_required
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
import secrets
import aiohttp

stores = Blueprint("stores", __name__)
load_dotenv()


"""
SHOPIFY OAUTH INTEGRATION DOCUMENT
https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/authorization-code-grant  
"""


@stores.post("/shopify/install-store")
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
        "scope": "read_orders,write_orders",
        "redirect_uri": f"{os.environ.get("APP_DOMAIN")}/shopify/auth/callback",
        "state": session.get("shopify_state")
    }

    url = f"https://{shop}/admin/oauth/authorize?{urlencode(params)}"
    return url


@stores.get("/shopify/auth/callback")
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
    await shopify_storesdb.Write.add_store(session.get("brand"), session.get("shopify_shop_name"), access_token)
    return redirect(os.environ.get("DASHBOARD_DOMAIN"))



@stores.route("/stores", methods=["POST"])
@login_required
@brand_required
async def add_store():
    """
    Add a new Shopify store for the logged-in user.

    Expected JSON payload:
    {
        "shopify_shop_name": "mystore",
        "shopify_access_token": "shpat_...",
        "store_name": "My Store" (optional)
    }

    Returns:
        JSON response with store data
    """
    try:
        user = session.get('user')

        data = await request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'Request body must be valid JSON'}), 400

        shopify_shop_name = str(data.get("shopify_shop_name", "")).strip()
        shopify_access_token = str(data.get("shopify_access_token", "")).strip()
        store_name = str(data.get("store_name", "")).strip()

        # Validate required fields
        if not shopify_shop_name or not shopify_access_token:
            return jsonify({
                'status': 'error',
                'message': 'shopify_shop_name and shopify_access_token are required'
            }), 400

        # Validate token before storing
        try:
            validate_shopify_token(shopify_shop_name, shopify_access_token)
        except ShopifyAPIError as exc:
            return jsonify({
                'status': 'error',
                'message': str(exc)
            }), 400

        # Add store to database
        result = await shopify_storesdb.Write.add_store(
            user_id=user,
            shopify_shop_name=shopify_shop_name,
            shopify_access_token=shopify_access_token,
            store_name=store_name or shopify_shop_name
        )

        if result['status'] == 'error':
            return jsonify(result), 400

        return jsonify({
            'status': 'success',
            'data': result['store']
        }), 201

    except Exception as e:
        print(f"Error adding store: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to add store: {str(e)}'
        }), 500


@stores.route("/stores", methods=["GET"])
async def list_stores():
    """
    List all Shopify stores for the logged-in user.

    Returns:
        JSON response with list of stores
    """
    try:
        user = session.get('user')
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

        user_stores = await shopify_storesdb.Fetch.get_user_stores(user)

        return jsonify({
            'status': 'success',
            'data': user_stores,
            'count': len(user_stores)
        }), 200

    except Exception as e:
        print(f"Error listing stores: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to list stores: {str(e)}'
        }), 500


@stores.route("/stores/<int:store_id>", methods=["GET"])
async def get_store(store_id):
    """
    Get details of a specific store.

    Args:
        store_id: ID of the store to retrieve

    Returns:
        JSON response with store data
    """
    try:
        user = session.get('user')
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

        store = await shopify_storesdb.Fetch.get_store_by_id(store_id, user)
        if not store:
            return jsonify({
                'status': 'error',
                'message': 'Store not found or access denied'
            }), 404

        return jsonify({
            'status': 'success',
            'data': store
        }), 200

    except Exception as e:
        print(f"Error retrieving store: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to retrieve store: {str(e)}'
        }), 500


@stores.route("/stores/<int:store_id>", methods=["PUT"])
async def update_store(store_id):
    """
    Update store details.

    Expected JSON payload:
    {
        "shopify_shop_name": "newshopname" (optional),
        "shopify_access_token": "shpat_..." (optional),
        "store_name": "New Store Name" (optional),
        "is_primary": true (optional)
    }

    Args:
        store_id: ID of the store to update

    Returns:
        JSON response with updated store data
    """
    try:
        user = session.get('user')
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

        data = await request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'Request body must be valid JSON'}), 400

        # Build update params
        update_params = {}
        allowed_fields = ['shopify_shop_name', 'shopify_access_token', 'store_name', 'is_primary']

        for field in allowed_fields:
            if field in data:
                update_params[field] = data[field]

        if not update_params:
            return jsonify({'status': 'error', 'message': 'No fields to update'}), 400

        result = await shopify_storesdb.Write.update_store(store_id, user, **update_params)

        if result['status'] == 'error':
            return jsonify(result), 400

        return jsonify({
            'status': 'success',
            'data': result['store']
        }), 200

    except Exception as e:
        print(f"Error updating store: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to update store: {str(e)}'
        }), 500


@stores.route("/stores/<int:store_id>", methods=["DELETE"])
async def delete_store(store_id):
    """
    Delete a store (soft delete - marks as inactive).

    Args:
        store_id: ID of the store to delete

    Returns:
        JSON response with status
    """
    try:
        user = session.get('user')
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

        result = await shopify_storesdb.Write.delete_store(store_id, user)

        if result['status'] == 'error':
            return jsonify(result), 400

        return jsonify(result), 200

    except Exception as e:
        print(f"Error deleting store: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to delete store: {str(e)}'
        }), 500


@stores.route("/stores/<int:store_id>/set-primary", methods=["POST"])
async def set_primary_store(store_id):
    """
    Set a store as the primary store for the user.

    Args:
        store_id: ID of the store to set as primary

    Returns:
        JSON response with updated store data
    """
    try:
        user = session.get('user')
        if not user:
            return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

        result = await shopify_storesdb.Write.update_store(store_id, user, is_primary=True)

        if result['status'] == 'error':
            return jsonify(result), 400

        return jsonify({
            'status': 'success',
            'data': result['store']
        }), 200

    except Exception as e:
        print(f"Error setting primary store: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to set primary store: {str(e)}'
        }), 500
