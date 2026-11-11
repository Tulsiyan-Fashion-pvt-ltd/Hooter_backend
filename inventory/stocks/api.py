from quart import Blueprint, jsonify, request, session
# import asyncio
from utils.prerequirements import login_required, brand_required
from . import mariadb
from catalog.products.authorize import product_api_access_required
from . import models
from . import services

stocks = Blueprint("stocks", __name__, url_prefix="/stocks")


@stocks.get("")
@login_required
@brand_required
async def get_inventory():
    """Get the total inventory including usku_id and stock"""
    args = request.args

    try:
        params = models.StocksRecordModel.model_validate(args).model_dump()
    except ValueError as e:
        return {'status': 'failed', 'message': 'Invalid parameters', 'code': '37281762', 'error': e.errors()}, 422

    records = await services.fetch_inventory_stocks_record(session.get('brand'), params.get('page'), 
                                                           params.get('records'), params.get('filter'),
                                                           params.get('type_id'), params.get('warehouse_id'))
    return jsonify(records[0]), records[1]
    

@stocks.get("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def get_product_stock(usku_id: str):
    """Fetches the stock for the given usku_id"""
    inventory = await mariadb.Fetch.product_stock(usku_id)
    if inventory == "error":
        return jsonify({"status": "failed", "message": "internal server error"}), 500

    if inventory is None:
        return jsonify({"status": "failed", "message": "Product stock not found for the given usku_id"}), 404
    return jsonify(inventory), 200



@stocks.get("/overview")
@login_required
@brand_required
async def inventory_overview():
    """Fetch inventory counts for a brand. for low, oos, sellable and total inventory"""
    stock = await mariadb.Fetch.stock_count(session.get('brand'))
    if stock == "error":
        return jsonify({"status": "failed", "message": "Internal server error while fetching inventory counts"}), 500
    return jsonify(stock), 200




# ********************************************************************************************
