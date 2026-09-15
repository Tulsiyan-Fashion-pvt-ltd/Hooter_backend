from quart import Blueprint, jsonify, request, session, json
# import asyncio
from utils.prerequirements import login_required, brand_required
from . import mariadb
from utils.helper import Helper
import re

supplier = Blueprint("supplier", __name__, url_prefix="/supplier")

@supplier.post("")
@login_required
@brand_required
async def add_supplier():
    payload = await request.get_json()

    accepted_payload = ["name", "number", "email", "house", "street", "locality", "city", "state", "pincode"]
    mandatory_payload = ["name", "number", "email", "locality", "city", "state", "pincode"]

    if not Helper.check_required_payload(payload, accepted_payload, mandatory_payload):
        return jsonify({"status": "invalid payload", "message": "payload is either missing mandatory payload or sending unaccepted payload"}), 400
    
    '''
        checking pincode
    '''
    pincode = str(payload.get("pincode"))
    pincode_regex = r'^\d{6}$'

    if not re.match(pincode_regex, pincode):
        return jsonify({"status": "invalid request", "message": "pincode should be 6 digit integer value"}), 406

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

    supplier_id = await mariadb.Write.supplier(data)
    if supplier_id == "error": 
        return jsonify({"status": "failed", "message": "failed to add the supplier"}), 500
    return jsonify({"status": "successful", "message": "added the supplier", "supplier_id": supplier_id}), 200

@supplier.get("")
@supplier.get("/<supplier_id>")
@login_required
@brand_required
async def get_suppliers(supplier_id = None):
    brand_id = session.get("brand")

    suppliers = None
    if supplier_id is None:
        suppliers = await mariadb.Fetch.suppliers(brand_id)
    else:
        suppliers = await mariadb.Fetch.supplier(brand_id, supplier_id)
    return jsonify(suppliers)
