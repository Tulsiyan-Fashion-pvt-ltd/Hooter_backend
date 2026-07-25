from quart import Blueprint, jsonify, request, session, json
# import asyncio
import inventory.routes as routes
from utils.prerequirements import login_required, brand_required
from inventory.warehouse import mariadb
from utils.helper import Helper
import re

warehouse    = Blueprint("warehouse", __name__, url_prefix="/warehouse")
                
@warehouse.post("")
@login_required
@brand_required
async def add_warehouse():
    brand_id = session.get("brand")

    payload = await request.get_json()

    accepted_payload = ["name", "number", "email", "house", "street", "locality", "city", "state", "pincode"]
    mandatory_payload = ["name", "number", "email", "locality", "city", "state", "pincode"]

    if not Helper.check_required_payload(payload, accepted_payload, mandatory_payload):
        return jsonify({"status": "denied", "msg": "invalid payload"}), 400
    
    pincode = str(payload.get("pincode"))
    pincode_regex = r'^\d{6}$'

    if not re.match(pincode_regex, pincode):
        return jsonify({"status": "invalid request", "msg": "pincode should be 6 digit integer value"}), 406
    
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
        return jsonify({"status": "failed", "msg": "failed to add the warehouse"}), 500
    return jsonify({"status": "successful", "msg": "added the warehouse", "warehouse_id": warehouse_id}), 200


@warehouse.get("")
@login_required
@brand_required
async def get_warehouses():
    brand_id = session.get("brand")
    warehouse_id = request.args.get("warehouse-id")

    if warehouse_id is None:
        warehouses = await mariadb.Fetch.warehouses(brand_id)
    else:
        warehouses = await mariadb.Fetch.warehouse(brand_id, warehouse_id)
    return jsonify(warehouses)