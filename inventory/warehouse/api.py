from quart import Blueprint, jsonify, request, session, json
# import asyncio
import inventory.routes as routes
from . import services
from utils.prerequirements import login_required, brand_required
from inventory.warehouse import mariadb
from utils.helper import Payload
from inventory.warehouse.authorize import warehouse_api_access_required
import re
from security_extensions import validate_csrf

warehouse = Blueprint("warehouse", __name__, url_prefix="/warehouse")
                
@warehouse.post("")
@validate_csrf
@login_required
@brand_required
async def add_warehouse():
    brand_id = session.get("brand")

    payload = await request.get_json()

    accepted_payload = ["name", "number", "email", "house", "street", "locality", "city", "state", "pincode"]
    mandatory_payload = ["name", "number", "email", "locality", "city", "state", "pincode"]

    if not (Payload.check_required_payload(payload, mandatory_payload) and
            Payload.check_accepted_payload(payload,accepted_payload)):
        return jsonify({"status": "denied", "message": "invalid payload"}), 400
    
    pincode = str(payload.get("pincode"))
    pincode_regex = r'^\d{6}$'

    if not re.match(pincode_regex, pincode):
        return jsonify({"status": "invalid request", "message": "pincode should be 6 digit integer value"}), 406
    
    data = {
        "brand_id": brand_id,
        "name": payload.get("name"),
        "number": payload.get("number"),
        "email": payload.get("email"),
        "address": json.dumps({
            "house": payload.get("house"),
            "street": payload.get("street"),
            "locality": payload.get("locality"),
            "city": payload.get("city"),
            "state": payload.get("state"),
            "pincode": payload.get("pincode")
        })
    }

    warehouse_id = await mariadb.Write.warehouse(data)
    if warehouse_id == "error": 
        return jsonify({"status": "failed", "message": "failed to add the warehouse"}), 500
    return jsonify({"status": "successful", "message": "added the warehouse", "warehouse_id": warehouse_id}), 200


@warehouse.get("")
@login_required
@brand_required
async def get_warehouses():
    brand_id = session.get("brand")

    warehouses = await mariadb.Fetch.warehouses(brand_id)
    if warehouses == "error":
        return jsonify({"status": "failed", "message": "Failed to fetch warehouses" }), 500
    return jsonify(warehouses)


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

    accepted_payload = ["name", "number", "email", "house", "street", "locality", "city", "state", "pincode"]
    if not Payload.check_accepted_payload(payload, accepted_payload):
        return jsonify({"status": "denied", "message": "Invalid payload"}), 400

    mandatory_payload = ["name", "number", "email", "locality", "city", "state", "pincode"]
    for key in payload.keys():
        if payload.get(key) in mandatory_payload and (payload.get(key)== None or len(payload.get(key)) == 0):
            return jsonify({"status": "failed", "message": "Provided value can not be null"}), 400
        
    logic_response = await services.update_warehouse(warehouse_id, payload)

    return jsonify(logic_response[0]), logic_response[1]