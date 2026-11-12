from . import mariadb
from datetime import datetime
from brand.auth.authorize import brand_access_required


@brand_access_required
async def fetch_inventory_stocks_record(brand_id: str, page: int = 1, limit: int = 10, 
                                        filter: str|None = None, type_id: str|None = None, warehouse_id: int| None = None) -> tuple[dict, int]:
    offset = (page - 1) * limit

    records = await mariadb.Fetch.inventory_stocks(brand_id, filter, type_id, limit, offset, warehouse_id)
    if records.get('status') == 'failed':
        return {'status': 'failed', 'message': 'Could not fetch the records', 'code': 'be387278'}, 500

    return records.get('records'), 200