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


@catalogue.route("/catalogue", methods=["POST"])
@_require_auth
def create_catalogue(user_id):
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
        store_id = data.get("store_id")
        variants = data.get("variants", [])
        images = data.get("images", [])

        # Validate required fields
        if not title or not description:
            return jsonify({
                "status": "error",
                "message": "title and description are required"
            }), 400

        if store_id is None:
            return jsonify({
                "status": "error",
                "message": "store_id is required"
            }), 400

        # Idempotency support
        idempotency_key = request.headers.get("Idempotency-Key")
        cached_response = CatalogueService._get_or_create_idempotency(
            idempotency_key=idempotency_key,
            user_id=user_id,
            store_id=store_id
        )
        if cached_response:
            return jsonify({"status": "success", "data": cached_response}), 200

        # Create catalogue with complete data
        result = CatalogueService.create_catalogue_complete(
            title=title,
            description=description,
            vendor=vendor,
            product_type=product_type,
            tags=tags,
            store_id=store_id,
            variants=variants,
            images=images,
            user_id=user_id
        )

        CatalogueService._store_idempotency_response(
            idempotency_key=idempotency_key,
            user_id=user_id,
            store_id=store_id,
            response_payload=result
        )

        return jsonify({
            "status": "success",
            "data": result
        }), 201

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


@catalogue.route("/catalogue/<catalogue_id>", methods=["GET"])
@_require_auth
def get_catalogue(user_id, catalogue_id):
    """
    Retrieve catalogue details by ID.
    User can only access their own catalogues.

    Args:
        catalogue_id: UUID of the catalogue

    Returns:
        JSON response with catalogue data
    """
    try:
        result = CatalogueService.get_catalogue_by_id(catalogue_id, user_id)

        if not result:
            return jsonify({
                "status": "error",
                "message": "Catalogue not found or access denied"
            }), 404

        return jsonify({
            "status": "success",
            "data": result
        }), 200

    except Exception as e:
        print(f"Error retrieving catalogue: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to retrieve catalogue: {str(e)}"
        }), 500


@catalogue.route("/catalogues", methods=["GET"])
@_require_auth
def list_catalogues(user_id):
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
        store_id = request.args.get("store_id", type=int)
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
        status = request.args.get("status")
        search = request.args.get("search")

        # Validate pagination parameters
        limit = min(limit, 100)  # Max 100 records per request
        offset = max(offset, 0)

        # If store_id provided, verify user owns it
        if store_id:
            store = Fetch.get_store_by_id(store_id, user_id)
            if not store:
                return jsonify({
                    "status": "error",
                    "message": "Store not found or access denied"
                }), 403

        results = CatalogueService.list_catalogues(
            user_id=user_id,
            store_id=store_id,
            limit=limit,
            offset=offset,
            status=status,
            search=search
        )

        return jsonify({
            "status": "success",
            "data": results,
            "count": len(results)
        }), 200

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


@catalogue.route("/catalogue/<catalogue_id>", methods=["PATCH"])
@_require_auth
def update_catalogue(user_id, catalogue_id):
    """Update catalogue and sync to Shopify."""
    try:
        data = request.get_json() or {}
        result = CatalogueService.update_catalogue(catalogue_id, user_id, data)
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


@catalogue.route("/catalogue/<catalogue_id>", methods=["DELETE"])
@_require_auth
def delete_catalogue(user_id, catalogue_id):
    """Delete or archive catalogue and Shopify product."""
    try:
        soft_delete = request.args.get("soft", "true").lower() == "true"
        result = CatalogueService.delete_catalogue(catalogue_id, user_id, soft_delete=soft_delete)
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


@catalogue.route("/catalogue/bulk", methods=["POST"])
@_require_auth
def bulk_create_catalogue(user_id):
    """Bulk create catalogues."""
    try:
        data = request.get_json() or {}
        items = data.get("items", [])
        if not items:
            return jsonify({"status": "error", "message": "items array required"}), 400

        results = []
        for item in items:
            result = CatalogueService.create_catalogue_complete(
                title=str(item.get("title", "")).strip(),
                description=str(item.get("description", "")).strip(),
                vendor=str(item.get("vendor", "")).strip(),
                product_type=str(item.get("product_type", "")).strip(),
                tags=str(item.get("tags", "")).strip(),
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
