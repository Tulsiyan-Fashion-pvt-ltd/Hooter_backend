from quart import Blueprint, session, request, jsonify, Response, abort, json
from catalog.products import mariadb
from brand.auth import mariadb as brand_sql
from catalog.categories import mongodb as categories
from catalog.products import mongodb 
from catalog.products.utils.id import create_usku, create_variant_id
from utils.prerequirements import login_required, brand_required
from utils import helper
from utils import workbook
from utils.helper import Payload
import asyncio
from catalog.providers.shopify import products as shopify_products
from config import _platforms, _product_image_bucket, _product_image_root_key
from . import services
import s3
from traceback import print_exc


products = Blueprint("products", __name__, url_prefix = "/products")


# check if the user has even added a single catalog or not.
@products.get('/if-exists')
@login_required
@brand_required
async def if_catalog_exists():
    is_catalog = await mariadb.Fetch.is_exists_catalog(session.get('brand'))
    if is_catalog == True:
        return jsonify({"catalog": "available"})
    else:
        return jsonify({"catalog": "unavailable"})


# upload single catalog to the hooter backend
@products.post('/single')
@login_required
@brand_required
async def upload_single_catalog():
    """
    UPLOAD SINGLE PRODUCT TO THE CATALOG
    """
    category_id = request.args.get('type-id', type=str)

    payload = await request.get_json()
    listing_attributes = dict(payload.get('listing_attributes'))
    product_attributes = dict(payload.get("category_attributes"))
    # variants = list(payload.get("variants"))

    # checking the payload
    system_keys = await asyncio.gather(categories.Fetch.Attributes.Catalog.all(),
                                       categories.Fetch.Attributes.Catalog.mandatory(),
                                       categories.Fetch.Attributes.Category(category_id).mandatory())


    catalog_accepted_keys = system_keys[0]
    catalog_mandatory_keys = system_keys[1]
    category_mandatory_keys = system_keys[2]

    if not (Payload.check_accepted_payload(listing_attributes, catalog_accepted_keys) and 
            Payload.check_required_payload(listing_attributes, catalog_mandatory_keys)):
        return jsonify({"status": "bad request", "message": "Invalid listing attributes"}), 400

    if category_mandatory_keys != [] and Payload.check_required_payload(product_attributes, category_mandatory_keys):
        # we're not checking the accepted once because there could be custom attributes
        return jsonify({"status": "bad request", "message": "Invalid product attributes"}), 400

    """creating product in the system"""
    response = await services.create_product(listing_attributes = listing_attributes,
                                   category_attributes = product_attributes,
                                   category_id = category_id
                                )

    if response.get("error"):
        return jsonify({"status": "failed", "message": response.get("error")}), response.get("code")

    return jsonify({"status": "successful", "message": "added the single catalog", "usku_id": response.get("usku_id")}), 200


"""UNDER CONSTRUCTION 🚧🚧🚧"""
# @products.post("/variants/<usku_id>")
# @login_required
# @brand_required
# async def upload_variants(usku_id: str):
#     variants = await request.get_json()

#     db = await services.create_variants(usku_id, variants)
#     if db.get("error"):
#         return jsonify({"status": "failed", "message": db.get("error")}), 400

#     return jsonify({"status": "successful", "message": db.get("message")}), 200
    



@products.post('/bulk')
@login_required
@brand_required
async def upload_bulk_catalog():
    """
    UPLOAD BULK PRODUCT USING XLSX EXCEL FILE
    """
    file_payload = await request.files

    xlsx_sheet = file_payload.get("sheet")
    type_id = request.args.get("type-id", type=str)

    '''
        checking the payload and files
    '''
    if type_id is None or xlsx_sheet is None:
        return jsonify({"status": "invalid form data"}), 400
    
    # checking the filename should be .xlsx file
    if not xlsx_sheet.filename.endswith(".xlsx"):
        return jsonify({"status": "invalid sheet", "error": "file should have .xlsx extension"}), 415
    

    ''' after verifying everything is correct '''

    ''' read the file and see if the necessary data is provided
        if not then exit the function 
    '''

    catalog_all_keys, catalog_mandatory_keys, category_mandatory_keys = await asyncio.gather(categories.Fetch.Attributes.Catalog.all(),
                                                                                            categories.Fetch.Attributes.Catalog.mandatory(), 
                                                                                             categories.Fetch.Attributes.Category(type_id).mandatory())
    expected_mandatory_keys  = catalog_mandatory_keys + category_mandatory_keys


    '''HEADER VERIFICATION'''
    document_header = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=1)
    if not set(expected_mandatory_keys).issubset(set(document_header)): # if mandatory keys exists
        return jsonify({"status": "failed", "message": "redownload the bulk upload file and re-upload"}), 422


    """iterating over the document and pushing in the system after verification"""

    
    services.upload_xlsx(xlsx_sheet, type_id, catalog_all_keys, expected_mandatory_keys)

    new_sheet = None # new sheet for the un-uploaded files




    if new_sheet != None:
        new_sheet = workbook.write(new_sheet, row=["Remove the Error column and this message while uploading again"]) # message for the new sheet
        return Response(new_sheet), 202
    
    return jsonify({"status": "ok"}), 200



# get the uploaded catalog products and status
@products.get("/<usku_id>")
@login_required
@brand_required
async def show_product(usku_id: str):
    
    product_data = await asyncio.gather(mariadb.Fetch.catalog_product(usku_id),
                                        mongodb.Fetch.catalog_product(usku_id))
    
    if product_data[0].get("error") or product_data[1].get("error"):
        return jsonify({"status": "failed", "message": "request failed"}), 400
    else:
        # print(product_data[1])
        product_data = product_data[0] | product_data[1]
        return jsonify(product_data), 200


@products.get("")
@login_required
@brand_required
async def list_products():
    """
    SERVING THE LISTS OF UPLOADED CATALOG IF USKU_ID IS PROVIDED
    """
    brand_id = session.get("brand")

    catalog_data = await asyncio.gather(mariadb.Fetch.catalog_upload_count(brand_id), 
                          mariadb.Fetch.catalog_list(brand_id))
    
    if catalog_data[0] == "error" or catalog_data[1] == "error":
        return jsonify({"status": "request failed", "message": "could not fetch the catalog data"}), 500
    
    return jsonify({"count": catalog_data[0], "catalog-list": catalog_data[1]}), 200



'''
    this route serves the resource to delete a product from the catalog
'''
@products.delete("/<usku_id>")
@login_required
@brand_required
async def delete_product(usku_id: str):
    # print(usku_id)
    if not usku_id:
        return jsonify({"status": "invalid request", "message": "usku-id not provided"}), 400
    
    logic_response = await services.delete_product(usku_id)
    return jsonify(logic_response[0]), logic_response[1]



'''THIS FUNCTION REQUIRE SOME CORRECTION'''
@products.put("/<usku_id>")
@login_required
@brand_required
async def update_catalog_data(usku_id):
    """
    UPDATES THE CATALOG PRODUCT DATA
    """
    payload = await request.get_json()

    type_id = request.args.get("id")
    data = payload.get("data")

    if type_id == None or data == None:
        return jsonify({"status": "failed", "message": "invalid payload"}), 400

    '''checking the payload'''
    payload_list = await asyncio.gather(categories.Fetch.attributes(type_id).all(),
                                  categories.Fetch.attributes(type_id).mandatory())
    
    accepted_payload = payload_list[0]
    mandatory_payload = payload_list[1]

    
    accepted_payload.append("discount")

    if not helper.Helper.check_required_payload(data, accepted_payload, mandatory_payload):
        return jsonify({"status": "failed", "message": "invalid payload"}), 400

    response = await services.update_product(usku_id, data, type_id)
    return jsonify(response[0]), response[1]


# mark the catalog upload as completed
'''this function is meant to call after the images and catalog upload is successfull'''
@products.put("/<usku_id>/completed")
@login_required
@brand_required
async def mark_complete(usku_id):
    """
    UPDATE THE PRODUCT UPLOAD STATUS AS COMPLETED 
    """

    if usku_id and await mariadb.Fetch.is_usku_id_exists(usku_id):
        db_query = await mariadb.Write.status_complete(usku_id)
        if db_query == "ok":
            return jsonify({"status": "successful", "message": "updated the catalog upload as completed"}), 200
        else:
            jsonify({"status": "failed", "message": "error encountered while updating the status as completed"}), 500
    else:
        return jsonify({"status": "failed request", "message": "usku_id does not exists"}), 400               
    return jsonify({"status": "request completed", "message": "reqeust completed without updating the status"}), 202



@products.put("/<usku_id>")
@login_required
@brand_required
async def upload_product_to_platforms(platform: str):
    """
    RESPONSIBLE FOR UPLOADING AND ENABLING THE PRODUCT ON THE PLATFORMS
    """
    #check the platform resource
    if not platform or platform not in _platforms:
        return abort(404)

    await shopify_products.upload_product()
    return