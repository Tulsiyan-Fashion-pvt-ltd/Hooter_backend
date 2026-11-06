from quart import Blueprint, jsonify, session
from .mariadb import Fetch

integrations_api = Blueprint("integrations_api", __name__)


@integrations_api.get("/")
async def get_integrations():

    brand_id = session.get("brand")

    if not brand_id:
        return jsonify({"status": "error", "message": "Brand not selected"}), 400

    integrations = await Fetch.get_brand_integrations(brand_id)

    if integrations is None:
        return jsonify({"status": "error", "message": "Unable to fetch integrations"}), 500

    return jsonify({"status": "success", "data": integrations}), 200