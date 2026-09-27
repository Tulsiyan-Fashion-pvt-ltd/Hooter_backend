from quart import Blueprint, jsonify, request, session, json
# import asyncio
import inventory.routes as routes
from . import services
from utils.prerequirements import login_required, brand_required
from inventory.warehouse import mariadb
from utils.helper import Payload
from inventory.warehouse.authorize import warehouse_api_access_required
from security_extensions import validate_csrf
from . import model

warehouse = Blueprint("warehouse", __name__, url_prefix="/warehouse")
                
@warehouse.post("")
@validate_csrf
@login_required
@brand_required
async def add_warehouse():
    payload = await request.get_json()
    try:
        payload = model.warehouse.model_validate(await request.get_json())
    except Exception as e:
        return jsonify({'status': 'denied', 'message': "Invalid payload", 'errors': [
                {
                    "field": err["loc"],
                    "message": err["msg"]
                }
                for err in e.errors(include_context=False)
            ]}), 422

    response = await services.upload_warehouse(session.get('brand'), payload.model_dump())
    return jsonify(response[0]), response[1]
    


@warehouse.get("")
@login_required
@brand_required
async def get_warehouses():
    brand_id = session.get("brand")

    warehouses_response = await services.get_warehouses(brand_id)
    return jsonify(warehouses_response[0]), warehouses_response[1]


@warehouse.get("/<warehouse_id>")
@login_required
@brand_required
@warehouse_api_access_required
async def get_warehouse(warehouse_id):
    '''Get warehouse data for the particular id'''
    warehouse = await mariadb.Fetch.warehouse(warehouse_id)
    if warehouse.get("error") != None:
        print(warehouse)
        return jsonify({"status": "failed", "message": "Failed to fetch warehouse" }), 500
    return jsonify(warehouse)


@warehouse.delete("/<warehouse_id>")
@validate_csrf
@login_required
@brand_required
@warehouse_api_access_required
async def delete_warehouse(warehouse_id:str):
    if not warehouse_id:
         return jsonify({"status": "invalid request", "message": "warehouse-id not provided"}), 400

    logic_response = await services.delete_warehouse(warehouse_id)

    return jsonify(logic_response[0]), logic_response[1]


@warehouse.put("/<warehouse_id>")
@validate_csrf
@login_required
@brand_required
@warehouse_api_access_required
async def update_warehouse(warehouse_id: str):
    if not warehouse_id:
        return jsonify({"status": "invalid request", "message": "Warehouse ID not provided"}), 400

    payload = await request.get_json()
    try:
        if payload.get('pincode') and not payload.get('state'):
            warehouse_address = await mariadb.Fetch.warehouse(warehouse_id)
            state = warehouse_address.get('state')
            payload = {**payload, 'state': state}
        elif payload.get('state') and not payload.get('pincode'):
            warehouse_address = await mariadb.Fetch.warehouse(warehouse_id)
            pincode = warehouse_address.get('pincode')
            payload = {**payload, 'pincode': pincode}

        print(payload)
        validated_payload = model.warehouse_update.model_validate(payload)
        data = validated_payload.model_dump(exclude_unset=True)
    except Exception as e:
        return jsonify({'status': 'failed', 'message': 'Invalid paylod', 
                       'error': [{
                            "field": error['loc'],
                            "message": error['msg']
                        }
                        for error in e.errors(include_context=False)]}), 422

    logic_response = await services.update_warehouse(warehouse_id, data)

    return jsonify(logic_response[0]), logic_response[1]