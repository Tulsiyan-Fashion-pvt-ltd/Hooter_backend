from quart import Blueprint, session, request, jsonify
from utils.prerequirements import login_required, brand_required
from sql_queries import branddb, catalogdb
from utils import helper, products
from datetime import datetime
from mongo_queries import mongo_catalogdb

catalog = Blueprint('catalog', __name__)

# check if the user has even added a single catalog or not.
@catalog.get('/catalog/if-exists')
@login_required
@brand_required
async def if_catalog_exists():
    is_catalog = await catalogdb.Fetch.is_exists_catalog(session.get('brand'))
    if is_catalog == True:
        return jsonify({"catalog": "available"})
    else:
        return jsonify({"catalog": "unavailable"})
    



@catalog.post('/catalog/single-catalog')
@login_required
@brand_required
async def upload_single_catalog():

    payload = await request.get_json()

    accepted_main_keys = ["type", "data"]
    if not helper.Helper.check_required_payload(payload, accepted_main_keys, accepted_main_keys):
        return jsonify({"status": "invalid payload"})

    data = payload.get('data')
    niche_type = payload.get('type')

    # check the product type related fields
    fields = await mongo_catalogdb.Fetch.catalog_attributes(niche_type)
    if fields.get('error') is not None:
        return jsonify({"status": "invalid value", "msg": "incorrect id"}), 400
    
    fields.pop("niche_id") # taking out the niche_id field from the attributes

    niche_specific_keys = fields.keys()
    accepted_data_keys = [
        "sku-id",
        "title",
        "price",
        "compared-price",
        "purchasing-cost",
        "vendor",
        "ean",
        "hsn",
        "net-weight",
        "dead-weight",
        "volumetric-weight",
        "brand-name"
    ]
    
    necessary_data_keys = [
        "sku-id",
        "title",
        "price",
        "compared-price",
        "vendor",
        "net-weight",
        "dead-weight",
        "volumetric-weight"
    ]

    mandatory_niche_attributes = [
        field for field in fields.keys() 
        if fields.get(field) == "*" or "*" in fields.get(field)
    ]

    accepted_data_keys.extend(niche_specific_keys)
    necessary_data_keys.extend(mandatory_niche_attributes)

    if not helper.Helper.check_required_payload(data, accepted_data_keys, necessary_data_keys):
        return jsonify({"status": "invalid payload"}), 400

    product_type = data.get("type")
    # print(session.get('brand'))
    brand_name = await branddb.Fetch.brand_name_by_id(session.get('brand'))

    catalog = {
        "brand_id": session.get('brand'),
        "usku_id": await products.create_usku(),
        "sku_id": data.get("sku-id"),
        "type_id": niche_type,
        "title": data.get('title'),
        "type": product_type,
        "price": data.get("price"),
        "comp_price": data.get("compared-price"),
        "purchasing_cost": data.get("purchasing-cost"),
        "vendor": data.get("vendor") if data.get("vendor") else brand_name, # if the vendor id is not there then enter the brand name
        "ean": data.get('ean'),
        "hsn": data.get("hsn"),
        "net_weight": data.get("net-weight"),
        "dead_weight": data.get("dead-weight"),
        "volumentric_weight": data.get("volumetric-weight"),
        "brand_name": data.get("brand-name") if data.get("brand-name") else brand_name,
        "update_timestamp": datetime.now()
    }

    response = await catalogdb.Write.add_single_catalog(catalog)
    # add the details in the mongo db

    if response != "ok":
        if response.get('error') == 1062:
            return jsonify({"status": "failed", "error": "duplicate sku id"}), 409
        else:
            return jsonify({"error encountered while adding the catalog"}), 500
    
    return jsonify({"Status": "successful", "message": "added the single catalog"})




# some data are niche specific soo for the front end to show them, it has to fetch it first
# this route will provide the data fields which for niche specific attributes
@catalog.get('/catalog/attribute-fields')
@login_required
@brand_required
async def get_attribute_fields():

    niche_id = request.args.get('niche')

    #sanitising the arguments
    if niche_id is None:
        return jsonify({'status': "invalid argument", "msg": "no niche field available, it should be ?niche=<id>"}), 400
    else:
        try:
            niche_id = int(niche_id)
        except Exception as e:
            return jsonify({"staus": "invalid value", "msg": "the id should be int type"})

    niche_attributes = await mongo_catalogdb.Fetch.catalog_attributes(niche_id)

    if niche_attributes.get('error') is not None:
        return jsonify({"status": "interrupted", "msg": "interal error"}), 500
    
    return jsonify(niche_attributes)