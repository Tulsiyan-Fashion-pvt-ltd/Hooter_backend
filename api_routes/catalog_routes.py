from quart import Blueprint, session, request, jsonify
from utils.prerequirements import login_required, brand_required
from sql_queries import branddb, catalogdb
from utils import helper, products
from datetime import datetime

catalog = Blueprint('catalog', __name__)

# check if the user has even added a single catalog or not.
@catalog.get('/if-catalog')
@login_required
@brand_required
async def if_catalog_exists():
    is_catalog = await catalogdb.Fetch.is_exists_catalog(session.get('brand'))
    if is_catalog == True:
        return jsonify({"catalog": "available"})
    else:
        return jsonify({"catalog": "unavailable"})
    

@catalog.post('/upload-single-catalog')
@login_required
@brand_required
async def upload_single_catalog():

    data = await request.get_json()

    accepted_keys = [
        "sku-id",
        "title",
        "type",
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

    necessary_keys = [
        "sku-id",
        "title",
        "type",
        "price",
        "compared-price",
        "vendor",
        "net-weight",
        "dead-weight",
        "volumetric-weight"
    ]

    if not helper.Helper.check_required_payload(data, accepted_keys, necessary_keys):
        return jsonify({"status": "invalid payload"})

    product_type = data.get("type")
    # print(session.get('brand'))
    brand_name = await branddb.Fetch.brand_name_by_id(session.get('brand'))

    catalog = {
        "brand_id": session.get('brand'),
        "usku_id": await products.create_usku(),
        "sku_id": data.get("sku-id"),
        "niche_id": 1,
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
    if response != "ok":
        return jsonify({"error encountered while adding the catalog"})
    
    return jsonify({"Status": "successful", "message": "added the single catalog"})
    pass
    

