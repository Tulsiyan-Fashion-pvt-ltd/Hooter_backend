from quart import Blueprint, request, jsonify, session, abort, redirect
import requests
from .. import mariadb
from ..helper import validate_shopify_token, ShopifyAPIError, verify_hmac
from utils.prerequirements import login_required, brand_required
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
import secrets
import aiohttp
from .. import shopify
load_dotenv()


@shopify.route("/shopify/stores", methods=["GET"])
@login_required
@brand_required
async def list_stores():
    """
    List all Shopify stores for the logged-in user.

    Returns:
        JSON response with list of stores
    """
    try:
        brand = session.get('brand')
        brand_stores = await mariadb.Fetch.get_brand_stores(brand)

        return jsonify({
            'status': 'success',
            'data': brand_stores,
            'count': len(brand_stores)
        }), 200

    except Exception as e:
        print(f"Error listing stores: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to list stores: {str(e)}'
        }), 500
    

@shopify.route("/shopify/stores/<int:store_id>", methods=["DELETE"])
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

        result = await mariadb.Write.delete_store(store_id, user)

        if result['status'] == 'error':
            return jsonify(result), 400

        return jsonify(result), 200

    except Exception as e:
        print(f"Error deleting store: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Failed to delete store: {str(e)}'
        }), 500


# @stores.route("/stores/<int:store_id>", methods=["GET"])
# @login_required
# @brand_required
# async def get_store(store_id):
#     """
#     Get details of a specific store.

#     Args:
#         store_id: ID of the store to retrieve

#     Returns:
#         JSON response with store data
#     """
#     try:
#         user = session.get('user')
#         if not user:
#             return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

#         store = await shopify_storesdb.Fetch.get_store_by_id(store_id, user)
#         if not store:
#             return jsonify({
#                 'status': 'error',
#                 'message': 'Store not found or access denied'
#             }), 404

#         return jsonify({
#             'status': 'success',
#             'data': store
#         }), 200

#     except Exception as e:
#         print(f"Error retrieving store: {str(e)}")
#         return jsonify({
#             'status': 'error',
#             'message': f'Failed to retrieve store: {str(e)}'
#         }), 500


# @stores.route("/stores/<int:store_id>", methods=["PUT"])
# async def update_store(store_id):
#     """
#     Update store details.

#     Expected JSON payload:
#     {
#         "shopify_shop_name": "newshopname" (optional),
#         "shopify_access_token": "shpat_..." (optional),
#         "store_name": "New Store Name" (optional),
#         "is_primary": true (optional)
#     }

#     Args:
#         store_id: ID of the store to update

#     Returns:
#         JSON response with updated store data
#     """
#     try:
#         user = session.get('user')
#         if not user:
#             return jsonify({'status': 'error', 'message': 'Unauthorized access'}), 401

#         data = await request.get_json()
#         if not data:
#             return jsonify({'status': 'error', 'message': 'Request body must be valid JSON'}), 400

#         # Build update params
#         update_params = {}
#         allowed_fields = ['shopify_shop_name', 'shopify_access_token', 'store_name', 'is_primary']

#         for field in allowed_fields:
#             if field in data:
#                 update_params[field] = data[field]

#         if not update_params:
#             return jsonify({'status': 'error', 'message': 'No fields to update'}), 400

#         result = await shopify_storesdb.Write.update_store(store_id, user, **update_params)

#         if result['status'] == 'error':
#             return jsonify(result), 400

#         return jsonify({
#             'status': 'success',
#             'data': result['store']
#         }), 200

#     except Exception as e:
#         print(f"Error updating store: {str(e)}")
#         return jsonify({
#             'status': 'error',
#             'message': f'Failed to update store: {str(e)}'
#         }), 500