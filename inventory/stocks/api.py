from quart import Blueprint, jsonify, request, session, json
# import asyncio
from utils.prerequirements import login_required, brand_required
from . import mariadb
from utils.helper import Helper
import re
from catalog.products.authorize import product_api_access_required

stocks = Blueprint("stocks", __name__, url_prefix="/stocks")


@stocks.get("")
@login_required
@brand_required
async def get_inventory():
    brand_id = session.get("brand")
    filter = request.args.get("filter")

    accepted_filters = ("sellable", "oos", "low-stock", None)
    if filter not in accepted_filters:
        return jsonify({"status": "invalid request", "msg": "not a valid filter"}), 400
     
    inventory = await mariadb.Fetch.inventory(brand_id, filter)
    if inventory == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    return jsonify(inventory), 200



@stocks.get("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def get_product_stock(usku_id: str):
    brand_id = session.get('brand')
    inventory = await mariadb.Fetch.inventory(brand_id, filter, usku_id)
    if inventory == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    return jsonify(inventory), 200

@stocks.get("/count")
@login_required
@brand_required
async def get_inventory_counts():
    """Fetch inventory counts for a brand."""
    brand_id = session.get("brand")

    stock = await mariadb.Fetch.stock_count(brand_id)
    return jsonify(stock), 200




# ********************************************************************************************
