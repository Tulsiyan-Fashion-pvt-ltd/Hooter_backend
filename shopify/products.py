import json
from quart import Blueprint, request, jsonify, session
from services.product_service import ProductService
from services.exceptions import AuthorizationError, ShopifyAPIError, ValidationError, IdempotencyConflict
from sql_queries.storesdb import Fetch
from utils.prerequirements import login_required

products = Blueprint("products", __name__)


@products.route("/products", methods=["POST"])
@login_required
async def create_product():
    try:
        user_id = session.get('user')
        data = await request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "Request body must be valid JSON"}), 400

        title = str(data.get("title", "")).strip()
        description = str(data.get("description", "")).strip()
        vendor = str(data.get("vendor", "")).strip()
        product_type = str(data.get("product_type", "")).strip()
        tags = str(data.get("tags", "")).strip()
        brand_id = data.get("brand_id")
        store_id = data.get("store_id")
        variants = data.get("variants", [])
        images = data.get("images", [])
        idempotency_key = request.headers.get("Idempotency-Key")

        if not title or not description:
            return jsonify({"status": "error", "message": "title and description are required"}), 400
        if brand_id is None or store_id is None:
            return jsonify({"status": "error", "message": "brand_id and store_id are required"}), 400

        try:
            result = await ProductService.create_product_complete(
                title=title,
                description=description,
                vendor=vendor,
                product_type=product_type,
                tags=tags,
                brand_id=brand_id,
                store_id=store_id,
                variants=variants,
                images=images,
                user_id=user_id,
                idempotency_key=idempotency_key
            )
        except IdempotencyConflict as ic:
            return jsonify({"status": "error", "message": "Idempotency conflict", "data": json.loads(str(ic))}), 409
        except AuthorizationError as ae:
            return jsonify({"status": "error", "message": str(ae)}), 403
        except ValidationError as ve:
            return jsonify({"status": "error", "message": "Validation failed", "errors": json.loads(str(ve)) if str(ve).startswith("{") else str(ve)}), 400
        except ShopifyAPIError as se:
            return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
        except Exception as exc:
            return jsonify({"status": "error", "message": f"Internal error: {exc}"}), 500

        return jsonify({"status": "success", **result}), 201

    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to create product: {str(e)}"}), 500


@products.route("/products", methods=["GET"])
@login_required
async def list_products():
    try:
        user_id = session.get('user')
        brand_id = request.args.get("brand_id", type=int)
        status = request.args.get("status")
        search = request.args.get("search")
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400

        result = await ProductService.list_products(
            brand_id=brand_id,
            user_id=user_id,
            status=status,
            search=search,
            limit=limit,
            offset=offset
        )
        return jsonify({"status": "success", "data": result, "count": len(result)}), 200

    except AuthorizationError as ae:
        return jsonify({"status": "error", "message": str(ae)}), 403
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to list products: {str(e)}"}), 500


@products.route("/products/<uid>", methods=["GET"])
@login_required
async def get_product_by_uid(uid):
    try:
        user_id = session.get('user')
        brand_id = request.args.get("brand_id", type=int)
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        try:
            product = await ProductService.get_product_by_uid(uid, brand_id, user_id)
        except AuthorizationError as ae:
            return jsonify({"status": "error", "message": str(ae)}), 403
        if not product:
            return jsonify({"status": "error", "message": "Product not found"}), 404
        return jsonify({"status": "success", "data": product}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to retrieve product: {str(e)}"}), 500


@products.route("/products/<uid>", methods=["PATCH"])
@login_required
async def update_product(uid):
    try:
        user_id = session.get('user')
        data = await request.get_json() or {}
        brand_id = data.get("brand_id")
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        try:
            result = await ProductService.update_product(uid, brand_id, user_id, data)
        except AuthorizationError as ae:
            return jsonify({"status": "error", "message": str(ae)}), 403
        except ShopifyAPIError as se:
            return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
        status_code = 200 if result.get("status") == "success" else 400
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to update product: {str(e)}"}), 500


@products.route("/products/<uid>", methods=["DELETE"])
@login_required
async def delete_product(uid):
    try:
        user_id = session.get('user')
        brand_id = request.args.get("brand_id", type=int)
        soft_delete = request.args.get("soft", "true").lower() == "true"
        if not brand_id:
            return jsonify({"status": "error", "message": "brand_id is required"}), 400
        try:
            result = await ProductService.delete_product(uid, brand_id, user_id, soft_delete=soft_delete)
        except AuthorizationError as ae:
            return jsonify({"status": "error", "message": str(ae)}), 403
        except ShopifyAPIError as se:
            return jsonify({"status": "error", "message": "Shopify API error", "errors": str(se)}), 502
        status_code = 200 if result.get("status") == "success" else 400
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"status": "error", "message": f"Failed to delete product: {str(e)}"}), 500
