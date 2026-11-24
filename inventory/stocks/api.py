from quart import Blueprint, jsonify, request, session
# import asyncio
from utils.prerequirements import login_required, brand_required
from . import mariadb
from catalog.products.authorize import product_api_access_required
from . import models
from . import services
from .authorize import stocks_api_access_required

stocks = Blueprint("stocks", __name__, url_prefix="/stocks")


@stocks.get("")
@login_required
@brand_required
async def get_inventory():
    """Get the total inventory including usku_id and stock"""
    args = request.args

    try:
        params = models.StocksRecordModel.model_validate(args).model_dump(exclude_unset=True)
    except ValueError as e:
        return {'status': 'failed', 'message': 'Invalid parameters', 'code': '37281762', 'error': e.errors(include_context=False)}, 422
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
    try:
        product_stock = await services.get_product_stock(usku_id)
        return jsonify(product_stock[0]), product_stock[1]
    except Exception as e:
        return jsonify({'status': 'failed', 'message': 'Unexpected internal server error', 'error': str(e), 'code': '34d9f173'}), 500



@stocks.get("/overview")
@login_required
@brand_required
async def inventory_overview():
    """Fetch inventory counts for a brand. for low, oos, sellable and total inventory"""
    stock = await mariadb.Fetch.stock_count(session.get('brand'))
    if stock == "error":
        return jsonify({"status": "failed", "message": "Internal server error while fetching inventory counts"}), 500
    return jsonify(stock), 200



@stocks.post("/adjust/<inventory_id>")
@login_required
@brand_required
@stocks_api_access_required
async def adjust_inventory(inventory_id):
    data = await request.get_json()

    try:
        data = models.AdjustInventoryModel.model_validate(data).model_dump()
        quantity = data.get("quantity")
    except ValueError as e:
        return ({'status': 'failed', 'message': 'Invalid Payload', 'error': e.errors(include_context=False), 'code': '389b9ffd'}), 422

    result = await services.adjust_stock(inventory_id, quantity)
    return jsonify(result[0]), result[1]
