from quart import Blueprint, jsonify, request, session, json
# import asyncio
from utils.prerequirements import login_required, brand_required
from sql_queries import inventorydb
from utils.helper import Helper
import re

inventory = Blueprint("inventory", __name__)

'''diff between inventory and catalog is catalog returns the product info without stock
    and inventory returns only the necessary details and the available stock
'''
@inventory.get("/inventory")
@login_required
@brand_required
async def get_inventory():
    brand_id = session.get("brand")

    filter = request.args.get("filter")
    accepted_filters = ("sellable", "oos", "low-stock", None)

    if filter not in accepted_filters:
        return jsonify({"status": "invalid request", "msg": "not a valid filter"}), 400
     
    inventory = await inventorydb.Fetch.inventory(brand_id, filter)
    if inventory == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    return jsonify(inventory), 200


@inventory.get("/inventory/stocks")
@login_required
@brand_required
async def get_inventory_counts():
    brand_id = session.get("brand")

    stock = await inventorydb.Fetch.stock_count(brand_id)
    return jsonify(stock), 200


@inventory.get("/inventory/inward-count")
@login_required
@brand_required
async def get_inward():
    brand_id = session.get("brand")

    inward = await inventorydb.Fetch.inward_count(brand_id)

    if inward == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    return jsonify(inward), 200


@inventory.get("/inventory/inward")
@login_required
@brand_required
async def inward_count():
    brand_id = session.get("brand")

    condition = request.args.get("type")
    # if not condition:
    #     condition = "completed"

    inward = await inventorydb.Fetch.inward(condition, brand_id)

    if inward == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    return jsonify(inward), 200


@inventory.post("/inventory/inward")
@login_required
@brand_required
async def create_inward():
    brand_id = session.get("brand")
    
    '''payload check'''
    payload = await request.get_json()
    accepted_payload = ["supplier_id", "usku_ids", "shipment", "warehouse"]
    mandatory_payload = accepted_payload

    if not Helper.check_required_payload(payload, accepted_payload, mandatory_payload):
        return jsonify({"status": "denied", "msg": "invalid payload"}), 400
    
    shipment_payload = payload.get("shipment")
    accepted_shipment_payload = ["shipment-ref", "vehicle-no", "transporter", "challan"]
    mandatory_payload_shipment = ["transporter"]

    if not Helper.check_required_payload(shipment_payload, accepted_shipment_payload, mandatory_payload_shipment):
        return jsonify({"status": "denied", "msg": "invalid payload"}), 400


    db_respose = await inventorydb.Write.inward(payload, brand_id)
    if db_respose == "error":
        return jsonify({"status": "failed", "msg": "unable to create the inward"}), 500
    
    return jsonify({"status": "successful", "inward-id": db_respose}), 200


@inventory.put("/inventory/inward")
@login_required
@brand_required
async def upload_inward():
    """
    UPLOAD THE INWARD DATA AS PARTIAL OR COMPLETE FOR THE GIVEN INWARD ID
    """

    inward_id = request.args.get("id")
    upload_type = request.args.get("type")

    if (not inward_id or not upload_type ) and upload_type not in ("partial", "completed"):
        return jsonify({"status": "rejected", "msg": "invalid request"}), 400
    
    payload = await request.get_json()
    accepted_paylaod = ["status", "id", "usku_ids"]
    mandatory_paylaod = accepted_paylaod

    if not Helper.check_required_payload(payload, accepted_paylaod, mandatory_paylaod):
        return jsonify({"status": "failed", "msg": "invalid payload"}), 422
    

    inward = {
        "inward_id": payload.get("id"),
        "status": payload.get("status"),
        "usku_ids": [{
            "usku_id": unit.get("usku_id"),
            "recieved": unit.get("recieved"),
            "shortage": unit.get("shortage"),
            "rejected": unit.get("rejected")
        }for unit in payload.get("usku_ids") if payload.get("usku_ids")]
    }

    db_query = await inventorydb.Write.inward(inward, session.get("brand"))
    if db_query != "ok": 
        return jsonify({"status": "failed", "msg": "could not process the request"}), 500
    return jsonify({"status": "successful", "msg": f"inward uploaded as {payload.get("status")}", "grn_id": db_query}), 200


@inventory.post("/inventory/supplier")
@login_required
@brand_required
async def add_supplier():
    payload = await request.get_json()

    accepted_payload = ["name", "number", "email", "house", "street", "locality", "city", "state", "pincode"]
    mandatory_payload = ["name", "number", "email", "locality", "city", "state", "pincode"]

    if not Helper.check_required_payload(payload, accepted_payload, mandatory_payload):
        return jsonify({"status": "invalid payload", "msg": "payload is either missing mandatory payload or sending unaccepted payload"}), 400
    
    '''
        checking pincode
    '''
    pincode = str(payload.get("pincode"))
    pincode_regex = r'^\d{6}$'

    if not re.match(pincode_regex, pincode):
        return jsonify({"status": "invalid request", "msg": "pincode should be 6 digit integer value"}), 406

    data = {
        "brand_id": session.get("brand"),
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

    supplier_id = await inventorydb.Write.supplier(data)
    if supplier_id == "error": 
        return jsonify({"status": "failed", "msg": "failed to add the supplier"}), 500
    return jsonify({"status": "successful", "msg": "added the supplier", "supplier_id": supplier_id}), 200


@inventory.get("/inventory/suppliers")
@login_required
@brand_required
async def get_suppliers():
    brand_id = session.get("brand")

    suppliers = await inventorydb.Fetch.suppliers(brand_id)
    return jsonify(suppliers)



@inventory.post("/inventory/warehouse")
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

    warehouse_id = await inventorydb.Write.warehouse(data)
    if warehouse_id == "error": 
        return jsonify({"status": "failed", "msg": "failed to add the warehouse"}), 500
    return jsonify({"status": "successful", "msg": "added the warehouse", "warehouse_id": warehouse_id}), 200


@inventory.get("/inventory/warehouses")
@login_required
@brand_required
async def get_warehouses():
    brand_id = session.get("brand")

    suppliers = await inventorydb.Fetch.warehouses(brand_id)
    return jsonify(suppliers)