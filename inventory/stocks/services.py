from . import mariadb
from datetime import datetime
from brand.auth.authorize import brand_access_required
from .authorize import stocks_access_required
from inventory.warehouse import services as warehouse_services
from catalog.products.authorize import product_access_required


@brand_access_required
async def fetch_inventory_stocks_record(brand_id: str, page: int = 1, limit: int = 10, 
                                        filter: str|None = None, type_id: str|None = None, warehouse_id: int| None = None) -> tuple[dict, int]:
    offset = (page - 1) * limit

    records = await mariadb.Fetch().inventory_stocks(brand_id, filter, type_id, limit, offset, warehouse_id)
    if records.get('status') == 'failed':
        return {'status': 'failed', 'message': 'Could not fetch the records', 'code': 'be387278'}, 500

    return records.get('records'), 200


@stocks_access_required
async def adjust_stock(inventory_id: int, quantity: int):
    result = await mariadb.Write().adjust_stock(inventory_id, quantity)

    if result is False:
        return {'status': 'failed', 'message': 'Inventory record not found or inactive'}, 404

    if result == "error":
        return {'status': 'failed', 'message': 'Could not adjust inventory stock'}, 500

    return {'status': 'success', 'message': 'Inventory stock adjusted successfully'}, 200


@product_access_required
async def get_product_stock(usku_id: str) -> tuple[dict, int]:
    inventory = await mariadb.Fetch().product_stock(usku_id)
    if inventory == "error":
        return {"status": "failed", "message": "internal server error"}, 500

    if inventory is None:
        return {"status": "failed", "message": "Product stock not found for the given usku_id"}, 404
    return inventory, 200