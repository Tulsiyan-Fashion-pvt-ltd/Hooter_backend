from quart import Blueprint, session, request, jsonify, Response, abort, url_for
from catalog.products import mariadb
from catalog.categories import mongodb as categories
from catalog.products import mongodb 
from utils.prerequirements import login_required, brand_required
from utils import helper
from utils.helper import Payload
import asyncio
from catalog.providers.shopify import products as shopify_products
from config import _platforms
from . import services
from traceback import print_exc
from uuid import uuid4
from .sse import product_sse
from .authorize import product_api_access_required




products = Blueprint("products", __name__, url_prefix = "/products")
products.register_blueprint(product_sse)


# check if the user has even added a single catalog or not.
@products.get('/exists')
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
        return jsonify({"status": "bad request", "message": "invalid listing attributes"}), 400

    if category_mandatory_keys != [] and Payload.check_required_payload(product_attributes, category_mandatory_keys):
        # we're not checking the accepted once because there could be custom attributes
        return jsonify({"status": "bad request", "message": "invalid category attributes"}), 400

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
    Recieves the bulk product xlsx sheet and runs a job to upload them one by one and returns a 
    server side event(sse) url if the background job runs successfully without any error
    """
    type_id = request.args.get("type-id", type=str)
    file_payload = await request.files
    xlsx_sheet = file_payload.get("sheet")

    '''checking the payload and files'''
    if type_id is None:
        return jsonify({"status": "invalid request", "message": "type id not provided"}), 400
    
    # checking the filename should be .xlsx file
    if not xlsx_sheet :
        return jsonify({"status": "invalid request", "message": "no sheet provided"}), 400
    
    if not xlsx_sheet.filename.endswith(".xlsx"):
        return jsonify({"status": "invalid sheet", "error": "file should have .xlsx extension"}), 415
    

    '''RUNNING JOB TO UPLOAD THE PRODUCTS INTO THE SYSTEM'''
    try:
        job_id = str(uuid4().hex)
        xlsx_sheet.seek(0)
        sheet_data = xlsx_sheet.stream.read()

        upload_task = asyncio.create_task(services.upload_xlsx(sheet_data, type_id, job_id=job_id), name=job_id)
        upload_task.job_id = job_id

        services.tasks[job_id] = {"status": "pending", "event": asyncio.Event(), "task": upload_task}

        upload_task.add_done_callback(services.on_task_complete)
        return jsonify({"status": "successful", "message": "XLSX sheet receieved to upload the products", 
                        "sse_url": url_for("catalog.products.product_sse.xlsx_upload_stream", job_id=job_id)}), 200

    except Exception as e:
        print(e)
        print_exc()
        print(f"error occuired while scheduling the tasks for xlsx products upload")
        return jsonify({"status": "failed", "message": "error occured while scheduling product uploads from the file"}), 500



@products.get('/error_sheet/<job_id>')
@login_required
@brand_required
async def send_error_sheet(job_id):
    """
    Downloads the xlsx error sheet for the given job id
    """
    if job_id not in services.tasks:
        return abort(404)

    if services.tasks.get(job_id).get("status") != "completed":
        return jsonify({"status": "pending", "message": "job has not finished yet"}), 202

    returned_result = services.tasks.get(job_id).get("task").result()
    if returned_result.get("code") == 200:
        return abort(404)
    
    elif returned_result.get("code") == 202:
        '''READ THE SHEET AND SEND IN BYTES'''
        returned_result.get("sheet").seek(0)
        sheet = returned_result.get("sheet").read()
        services.tasks.pop(job_id)
        return Response(sheet, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": "attachment; filename=error_sheet.xlsx"})
    else:
        return jsonify({"status": "failed", "message": "unexpected error occured in the server"}), 500


@products.get("")
@login_required
@brand_required
async def list_products():
    """
    SERVING THE LISTS OF UPLOADED CATALOG
    """
    brand_id = session.get("brand")

    catalog_data = await asyncio.gather(mariadb.Fetch.catalog_upload_count(brand_id), 
                          mariadb.Fetch.catalog_list(brand_id))
    
    if catalog_data[0] == "error" or catalog_data[1] == "error":
        return jsonify({"status": "failed", "message": "could not fetch the catalog data"}), 500
    
    return jsonify({"count": catalog_data[0], "catalog_list": catalog_data[1]}), 200



@products.get("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def show_product(usku_id: str):
    '''get the uploaded catalog products and status'''
    product_data = await asyncio.gather(mariadb.Fetch.catalog_product(usku_id),
                                        mongodb.Fetch.catalog_product(usku_id))
    
    if product_data[0].get("error") or product_data[1].get("error"):
        return jsonify({"status": "failed", "message": "request failed"}), 400
    else:
        # print(product_data[1])
        product_data = product_data[0] | product_data[1]
        return jsonify(product_data), 200



@products.delete("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def delete_product(usku_id: str):
    '''Deletes the specified usku id9'''
    # print(usku_id)
    if not usku_id:
        return jsonify({"status": "invalid request", "message": "usku-id not provided"}), 400
    
    logic_response = await services.delete_product(usku_id)
    return jsonify(logic_response[0]), logic_response[1]



'''THIS FUNCTION REQUIRE SOME CORRECTION'''
@products.put("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def update_catalog_data(usku_id):
    """
    UPDATES THE CATALOG PRODUCT DATA
    """
    payload = await request.get_json()

    type_id = request.args.get("type-id")
    listing_attributes = payload.get("listing_attributes")
    category_attributes = payload.get("category_attributes")

    if type_id == None:
        return jsonify({"status": "failed", "message": "invalid payload"}), 400

    '''Need to check if the altered data is a mandatory field and the value is none.
    If so then the operation can not happen
    '''
    payload_list = await asyncio.gather(categories.Fetch.Attributes.Catalog.all(),
                                  categories.Fetch.Attributes.Catalog.mandatory(),
                                  categories.Fetch.Attributes.Category(type_id).mandatory())
    
    accepted_listing_keys = payload_list[0]
    mandatory_listing_keys = payload_list[1]
    mandatory_category_keys = payload_list[2]

    '''Checking if there any unknown attribute for listing'''
    if not set(listing_attributes.keys()).issubset(accepted_listing_keys):
        return jsonify({'status': "failed", "message": "Invalid value listing attribute"}), 422

    if not (all([listing_attributes.get(key) for key in mandatory_listing_keys ]) and 
            all([category_attributes.get(key) for key in mandatory_category_keys])):
        return jsonify({'status': "failed", "message": "Mandatory attribute can not be null"}), 422


    
    
    response = await services.update_product(usku_id, listing_attributes, category_attributes)
    return jsonify(response[0]), response[1]




# @products.put("/<usku_id>")
# @login_required
# @brand_required
# @product_api_access_required
# async def upload_product_to_platforms(platform: str):
#     """
#     RESPONSIBLE FOR UPLOADING AND ENABLING THE PRODUCT ON THE PLATFORMS
#     """
#     #check the platform resource
#     if not platform or platform not in _platforms:
#         return abort(404)

#     await shopify_products.upload_product()
    # return