from quart import Blueprint, jsonify, request, session, json
# import asyncio
import inventory
from utils.prerequirements import login_required, brand_required
from . import mariadb
from utils.helper import Helper
import re

inward = Blueprint("inward", __name__, url_prefix="/inward")


@inward.get("/count")
@login_required
@brand_required
async def get_inward():
    brand_id = session.get("brand")

    inward = await mariadb.Fetch.inward_count(brand_id)

    if inward == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    return jsonify(inward), 200


@inward.get("")
@login_required
@brand_required
async def inward_count():
    brand_id = session.get("brand")

    inward_id = request.args.get("id")
    print(inward_id)

    if inward_id:
        inward = await mariadb.Fetch.inward(None, brand_id, inward_id)
    else:
        condition = request.args.get("type")
        inward = await mariadb.Fetch.inward(condition, brand_id)

    if inward == "error":
        return jsonify({"status": "failed", "msg": "internal server error"}), 500
    elif inward == "not allowed":
        return jsonify({"status": "failed", "msg": "The inward has already completed"}), 403
    return jsonify(inward), 200


@inward.post("")
@login_required
@brand_required
async def create_inward():
    brand_id = session.get("brand")
    
    '''payload check'''
    payload = await request.get_json()
    print(payload)
    accepted_payload = ["supplier_id", "usku_ids", "shipment", "warehouse_id"]
    mandatory_payload = accepted_payload

    if not Helper.check_required_payload(payload, accepted_payload, mandatory_payload):
        return jsonify({"status": "denied", "msg": "invalid payload"}), 400
    
    shipment_payload = payload.get("shipment")
    accepted_shipment_payload = ["shipment-ref", "vehicle-no", "transporter", "challan", "arrival-date"]
    mandatory_payload_shipment = ["transporter"]

    if not Helper.check_required_payload(shipment_payload, accepted_shipment_payload, mandatory_payload_shipment):
        return jsonify({"status": "denied", "msg": "invalid shipment payload"}), 400


    db_respose = await mariadb.Write.inward(payload, brand_id)
    if db_respose == "error":
        return jsonify({"status": "failed", "msg": "unable to create the inward"}), 500
    
    return jsonify({"status": "successful", "inward-id": db_respose}), 200


@inward.put("")
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
    accepted_paylaod = ["usku_ids"]
    mandatory_paylaod = accepted_paylaod

    if not Helper.check_required_payload(payload, accepted_paylaod, mandatory_paylaod):
        return jsonify({"status": "failed", "msg": "invalid payload"}), 422
    

    inward = {
        "inward_id": inward_id,
        "status": upload_type,
        "usku_ids": [{
            "usku_id": usku_id,
            "received": unit.get("received"),
            "rejected": unit.get("rejected"),
        }for usku_id, unit in payload.get("usku_ids").items() if payload.get("usku_ids")]
    }

    db_query = await mariadb.Update.inward(inward, session.get("brand"))
    if db_query == "error": 
        return jsonify({"status": "failed", "msg": "could not process the request"}), 500
    return jsonify({"status": "successful", "msg": f"inward uploaded as {payload.get("status")}", "grn_id": db_query}), 200
