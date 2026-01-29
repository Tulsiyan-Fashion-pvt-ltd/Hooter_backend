import os
import json
from flask import Blueprint, request, jsonify
from services.catalogue_service import CatalogueService

catalogue = Blueprint("catalogue", __name__)


@catalogue.route("/catalogue", methods=["POST"])
def create_catalogue():
    """
    Create a new catalogue entry and sync to Shopify.

    Expected JSON payload:
    {
        "title": "Product Title",
        "description": "Product Description",
        "price": "99.99",
        "user_id": "optional_user_id"
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
        price = data.get("price")
        user_id = data.get("user_id")

        # Load Shopify credentials from environment
        shopify_config = {
            "shop_name": os.environ.get("SHOPIFY_SHOP_NAME"),
            "access_token": os.environ.get("SHOPIFY_ACCESS_TOKEN")
        }

        # Validate Shopify config
        if not shopify_config["shop_name"] or not shopify_config["access_token"]:
            return jsonify({
                "status": "error",
                "message": "Shopify credentials not configured"
            }), 500

        # Create catalogue
        result = CatalogueService.create_catalogue(
            title=title,
            description=description,
            price=price,
            shopify_config=shopify_config,
            user_id=user_id
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

    except Exception as e:
        # Log exception for debugging
        print(f"Error creating catalogue: {str(e)}")

        return jsonify({
            "status": "error",
            "message": f"Failed to create catalogue: {str(e)}"
        }), 500


@catalogue.route("/catalogue/<catalogue_id>", methods=["GET"])
def get_catalogue(catalogue_id):
    """
    Retrieve catalogue details by ID.

    Args:
        catalogue_id: UUID of the catalogue

    Returns:
        JSON response with catalogue data
    """
    try:
        result = CatalogueService.get_catalogue_by_id(catalogue_id)

        if not result:
            return jsonify({
                "status": "error",
                "message": "Catalogue not found"
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
def list_catalogues():
    """
    List all catalogues with optional filtering.

    Query parameters:
        - user_id: Filter by user ID (optional)
        - limit: Number of records (default: 50)
        - offset: Pagination offset (default: 0)

    Returns:
        JSON response with catalogue list
    """
    try:
        user_id = request.args.get("user_id")
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))

        # Validate pagination parameters
        limit = min(limit, 100)  # Max 100 records per request
        offset = max(offset, 0)

        results = CatalogueService.list_catalogues(
            user_id=user_id,
            limit=limit,
            offset=offset
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
