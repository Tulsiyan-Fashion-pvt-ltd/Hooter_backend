from quart import Blueprint, jsonify, session
from .mariadb import Fetch
from . import services
from utils.prerequirements import login_required, brand_required

integrations_api = Blueprint("integrations_api", __name__, url_prefix = "/integrations")


@integrations_api.get("/")
@login_required
@brand_required
async def get_integrations():
    brand_id = session.get("brand")
    integrations = await services.get_connections(brand_id)

    if integrations[1] != 200:
        return jsonify({"status": "error", "message": "Unable to fetch integrations", 'code': '88d1ed65'}), 500

    return jsonify(integrations[0]), 200