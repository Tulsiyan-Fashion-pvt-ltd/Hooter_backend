import json
import base64
import hmac
import hashlib
import os
from flask import Blueprint, request, jsonify, session
from services.catalogue_service import CatalogueService
from services.exceptions import AuthorizationError, ShopifyAPIError, ValidationError
from database import Fetch

catalogue = Blueprint("catalogue", __name__)


def _get_authenticated_user():
    """
    Get authenticated user ID from session.
    Returns user_id or None if not authenticated.
    """
    return session.get('user')


def _require_auth(f):
    """Decorator to require authentication for endpoints."""
    from functools import wraps
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = _get_authenticated_user()
        if not user_id:
            return jsonify({
                "status": "error",
                "message": "Authentication required"
            }), 401
        return f(user_id, *args, **kwargs)
    return decorated_function


@catalogue.route("/products", methods=["POST"])
@_require_auth
def create_product(user_id):
    """
    Create a new catalogue entry with variants and images, then sync to Shopify.

    Expected JSON payload:
    {
        "title": "Product Title",
        "description": "Product Description",
        "vendor": "Vendor Name",
        "product_type": "Product Type",
        "tags": "tag1,tag2,tag3",
        "store_id": 123,
        "variants": [
            {
                "sku": "PROD-VAR-001",
                "price": "99.99",
                "compare_at_price": "149.99",
                "title": "Option Value",
                "weight": 2.5
            }
        ],
        "images": [
            {
                "image_url": "https://drive.google.com/uc?export=view&id=...",
                "alt_text": "Product image",
                "position": 0
            }
        ]
    }

    Returns:
        JSON response with catalogue data and Shopify mapping
    """
    try:
        # Parse JSON payload
        data = request.get_json()

        if not data:
            return jsonify({
                "status": "error",
                "message": "Request body must be valid JSON"
            }), 400

        # Extract fields
        title = str(data.get("title", "")).strip()
        description = str(data.get("description", "")).strip()
        vendor = str(data.get("vendor", "")).strip()
        product_type = str(data.get("product_type", "")).strip()
        tags = str(data.get("tags", "")).strip()
        brand_id = data.get("brand_id")
        store_id = data.get("store_id")
        variants = data.get("variants", [])
        images = data.get("images", [])
        if not title or not description:
            return jsonify({"status": "error", "message": "title and description are required"}), 400
        if brand_id is None or store_id is None:
            return jsonify({"status": "error", "message": "brand_id and store_id are required"}), 400
        try:
            result = CatalogueService.create_product_complete(
                title=title,
                description=description,
                vendor=vendor,
                product_type=product_type,
                tags=tags,
                brand_id=brand_id,
                store_id=store_id,
                variants=variants,
                images=images,
                user_id=user_id
            )
        except (AuthorizationError, ShopifyAPIError, ValidationError) as exc:
            return jsonify({"status": "error", "message": str(exc)}), 400
        except Exception as exc:
            return jsonify({"status": "error", "message": f"Internal error: {exc}"}), 500
        return jsonify({"status": "success", **result}), 201

    except ValueError as ve:
        # Validation error
        try:
            errors = json.loads(str(ve))
            return jsonify({
                "status": "error",
                "message": "Validation failed",
                "errors": errors
            }), 400
        except:
            return jsonify({
                "status": "error",
                "message": str(ve)
            }), 400

    except ValidationError as ve:
        return jsonify({
            "status": "error",
            "message": "Validation failed",
            "errors": json.loads(str(ve)) if str(ve).startswith("{") else str(ve)
        }), 400

    except AuthorizationError as ae:
        return jsonify({
            "status": "error",
            "message": str(ae)
        }), 403

    except ShopifyAPIError as se:
        return jsonify({
            "status": "error",
            "message": "Shopify API error",
            "errors": str(se)
        }), 502

    except Exception as e:
        # Log exception for debugging
        print(f"Error creating catalogue: {str(e)}")

        return jsonify({
            "status": "error",
            "message": f"Failed to create catalogue: {str(e)}"
        }), 500


@catalogue.route("/products/<uid>", methods=["GET"])
@_require_auth
def get_product_by_uid(user_id, uid):
    """
    Retrieve catalogue details by ID.
    User can only access their own catalogues.

    Args:
        catalogue_id: UUID of the catalogue

    Returns:
        JSON response with catalogue data
    """
    try:
        brand_id = request.args.get("brand_id", type=int)
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        result = CatalogueService.get_product_by_uid(uid, brand_id, user_id)
        if not result:
            return jsonify({"status": "error", "message": "Product not found"}), 404
        return jsonify({"status": "success", "data": result}), 200

    except Exception as e:
        print(f"Error retrieving catalogue: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to retrieve catalogue: {str(e)}"
        }), 500


@catalogue.route("/products", methods=["GET"])
@_require_auth
def list_products(user_id):
    """
    List catalogues for authenticated user.

    Query parameters:
        - store_id: Filter by specific store (optional)
        - limit: Number of records (default: 50)
        - offset: Pagination offset (default: 0)

    Returns:
        JSON response with catalogue list
    """
    try:
        brand_id = request.args.get("brand_id", type=int)
        status = request.args.get("status")
        search = request.args.get("search")
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        result = CatalogueService.list_products(
            brand_id=brand_id,
            user_id=user_id,
            status=status,
            search=search,
            limit=limit,
            offset=offset
        )
        return jsonify({"status": "success", "data": result, "count": len(result)}), 200

    except ValueError as ve:
        return jsonify({
            "status": "error",
            "message": f"Invalid parameter: {str(ve)}"
        }), 400

    except Exception as e:
        print(f"Error listing catalogues: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to list catalogues: {str(e)}"
        }), 500


@catalogue.route("/stores", methods=["GET"])
@_require_auth
def get_user_stores(user_id):
    """
    Get all stores for the authenticated user.

    Returns:
        JSON response with list of user's stores
    """
    try:
        stores = Fetch.get_user_stores(user_id)
        
        return jsonify({
            "status": "success",
            "data": stores,
            "count": len(stores)
        }), 200

    except Exception as e:
        print(f"Error retrieving user stores: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to retrieve stores: {str(e)}"
        }), 500


@catalogue.route("/products/<uid>", methods=["PATCH"])
@_require_auth
def update_product(user_id, uid):
    """Update catalogue and sync to Shopify."""
    try:
        data = request.get_json() or {}
        brand_id = data.get("brand_id")
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        result = CatalogueService.update_product(uid, brand_id, user_id, data)
        status_code = 200 if result.get("status") == "success" else 400
        return jsonify(result), status_code
    except AuthorizationError as ae:
        return jsonify({"status": "error", "message": str(ae)}), 403
    except ShopifyAPIError as se:
        return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to update catalogue: {str(e)}"
        }), 500


@catalogue.route("/products/<uid>", methods=["DELETE"])
@_require_auth
def delete_product(user_id, uid):
    """Delete or archive catalogue and Shopify product."""
    try:
        brand_id = request.args.get("brand_id", type=int)
        soft_delete = request.args.get("soft", "true").lower() == "true"
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        result = CatalogueService.delete_product(uid, brand_id, user_id, soft_delete=soft_delete)
        status_code = 200 if result.get("status") == "success" else 400
        return jsonify(result), status_code
    except AuthorizationError as ae:
        return jsonify({"status": "error", "message": str(ae)}), 403
    except ShopifyAPIError as se:
        return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to delete catalogue: {str(e)}"
        }), 500


@catalogue.route("/catalogue/<catalogue_id>/inventory", methods=["POST"])
@_require_auth
def sync_inventory(user_id, catalogue_id):
    """Sync inventory for catalogue variants."""
    try:
        data = request.get_json() or {}
        quantities = data.get("quantities", {})
        result = CatalogueService.sync_inventory_for_catalogue(catalogue_id, user_id, quantities)
        status_code = 200 if result.get("status") == "success" else 400
        return jsonify(result), status_code
    except AuthorizationError as ae:
        return jsonify({"status": "error", "message": str(ae)}), 403
    except ShopifyAPIError as se:
        return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to sync inventory: {str(e)}"
        }), 500


@catalogue.route("/products/bulk", methods=["POST"])
@_require_auth
def bulk_create_product(user_id):
    """Bulk create catalogues."""
    try:
        data = request.get_json() or {}
        items = data.get("items", [])
        if not items:
            return jsonify({"status": "error", "message": "items array required"}), 400

        results = []
        for item in items:
            result = CatalogueService.create_product_complete(
                title=str(item.get("title", "")).strip(),
                description=str(item.get("description", "")).strip(),
                vendor=str(item.get("vendor", "")).strip(),
                product_type=str(item.get("product_type", "")).strip(),
                tags=str(item.get("tags", "")).strip(),
                brand_id=item.get("brand_id"),
                store_id=item.get("store_id"),
                variants=item.get("variants", []),
                images=item.get("images", []),
                user_id=user_id
            )
            results.append(result)

        return jsonify({"status": "success", "data": results, "count": len(results)}), 201
    except ValidationError as ve:
        return jsonify({
            "status": "error",
            "message": "Validation failed",
            "errors": json.loads(str(ve)) if str(ve).startswith("{") else str(ve)
        }), 400
    except AuthorizationError as ae:
        return jsonify({"status": "error", "message": str(ae)}), 403
    except ShopifyAPIError as se:
        return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed bulk create: {str(e)}"
        }), 500


def _verify_shopify_webhook(request) -> bool:
    secret = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
    if not secret:
        return False
    hmac_header = request.headers.get("X-Shopify-Hmac-SHA256", "")
    digest = hmac.new(secret.encode(), request.get_data(), hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed, hmac_header)


@catalogue.route("/webhooks/shopify/product-update", methods=["POST"])
def webhook_product_update():
    """Handle Shopify product update webhooks."""
    if not _verify_shopify_webhook(request):
        return jsonify({"status": "error", "message": "invalid signature"}), 401

    payload = request.get_json() or {}
    shopify_product_id = payload.get("id")
    if not shopify_product_id:
        return jsonify({"status": "error", "message": "missing product id"}), 400

    result = CatalogueService.update_catalogue_from_webhook(payload)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code


@catalogue.route("/webhooks/shopify/inventory-update", methods=["POST"])
def webhook_inventory_update():
    """Handle Shopify inventory updates webhooks."""
    if not _verify_shopify_webhook(request):
        return jsonify({"status": "error", "message": "invalid signature"}), 401

    payload = request.get_json() or {}
    result = CatalogueService.update_inventory_from_webhook(payload)
    status_code = 200 if result.get("status") == "success" else 400
    return jsonify(result), status_code
