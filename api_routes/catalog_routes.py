from quart import Blueprint, session, request, jsonify, Response, current_app, abort
from utils.prerequirements import login_required, brand_required
from sql_queries import branddb, catalogdb
from utils import helper, products
from utils import sheets
from utils import imageio
from datetime import datetime
from mongo_queries import mongo_catalogdb
import asyncio
from collections import Counter

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
    

'''get the niche, subniche and categories'''
@catalog.get("/catalog/niche-data")
@login_required
@brand_required
async def get_niche_data():
    niches = await catalogdb.Fetch.niches()
    print(niches)

    try:
        niche_data ={
                        niche.get("niche_id"): {
                            "niche": niche.get("niche"),
                            "subniches": {
                                sub_niche.get("subniche_id"):{
                                    "subniche": sub_niche.get("subniche_name"),
                                    "categories": {
                                        category.get("category_id"): {
                                            "category": category.get("category_name"),
                                            "products": {
                                                product.get("type_id"): {
                                                    "product": product.get("product_name")
                                                }
                                                for product in await catalogdb.Fetch.niche_products(category.get("category_id"))
                                            }
                                        }
                                        for category in await catalogdb.Fetch.niche_categories(sub_niche.get("subniche_id"))
                                    }
                                }
                                for sub_niche in await catalogdb.Fetch.sub_niches(niche.get("niche_id"))
                            }

                        }
                        for niche in niches
                    }
    except Exception as e:
        return jsonify({"error": "failed", "msg": "could not complete the request"}), 500

    return jsonify({"niche_data": niche_data}), 200


# upload single catalog to the hooter backend
@catalog.post('/catalog/single-catalog')
@login_required
@brand_required
async def upload_single_catalog():

    payload = await request.get_json()

    accepted_main_keys = ["type", "data"]
    if not helper.Helper.check_required_payload(payload, accepted_main_keys, accepted_main_keys):
        return jsonify({"status": "invalid payload", "missing keys": necessary_data_keys})

    data = payload.get('data')
    niche_type = payload.get('type')

    try:
        niche_type = int(niche_type)
    except:
        return jsonify({"status": "invalid argument"}), 400

    #checking the payload 
    accepted_data_keys = await mongo_catalogdb.Fetch.attributes(niche_type).all()
    necessary_data_keys = await mongo_catalogdb.Fetch.attributes(niche_type).mandatory()

    if not helper.Helper.check_required_payload(data, accepted_data_keys, necessary_data_keys):
        return jsonify({"status": "invalid payload", "accepted_keys": accepted_data_keys, "mandatory": necessary_data_keys}), 400

    
    ## ADDING THE THE DATA IN THE SQL
    brand_name = await branddb.Fetch.brand_name_by_id(session.get('brand'))

    catalog = {
        "brand_id": session.get('brand'),
        "usku_id": await products.create_usku(),
        "sku_id": data.get("sku-id"),
        "type_id": niche_type,
        "title": data.get('title'),
        "price": data.get("price"),
        "compared-price": data.get("compared-price"),
        "purchasing_cost": data.get("purchasing-cost"),
        "vendor": data.get("vendor") if data.get("vendor") else brand_name, # if the vendor id is not there then enter the brand name
        "ean": data.get('ean'),
        "hsn": data.get("hsn"),
        "net_weight": data.get("net-weight"),
        "dead_weight": data.get("dead-weight"),
        "volumentric_weight": data.get("volumetric-weight"),
        "brand_name": data.get("brand-name") if data.get("brand-name") else brand_name
        # "update_timestamp": datetime.now()
    }

    response = await catalogdb.Write.catalog(catalog)

    if response != "ok":
        if response.get('error') == 1062:
            return jsonify({"status": "failed", "error": "duplicate sku id"}), 409
        else:
            return jsonify({"error encountered while adding the catalog"}), 500
    

    # add the details in the mongo db
    mongo_catalog_data = {"niche_id": niche_type}
    niche_specific_keys = await mongo_catalogdb.Fetch.attributes(niche_type).niche_specific()
    for key in niche_specific_keys:
        mongo_catalog_data[key] = data.get(key)

    await mongo_catalogdb.Write.single_catalog(mongo_catalog_data)
    
    return jsonify({"Status": "successful", "message": "added the single catalog"}), 200



# upload bulk catalog to the hooter backend
@catalog.post('/catalog/bulk-catalog')
@login_required
@brand_required
async def upload_bulk_catalog():
    file_payload = await request.files
    json_payload = await request.form

    xlsx_sheet = file_payload.get("sheet")
    type_id = json_payload.get("type")

    '''
        checking the payload and files
    '''
    if type_id is None or xlsx_sheet is None:
        return jsonify({"status": "invalid form data"}), 400
    
    # checking the filename should be .xlsx file
    if not xlsx_sheet.filename.endswith(".xlsx"):
        return jsonify({"status": "invalid sheet", "error": "file should have .xlsx extension"}), 415
    
    # checking if the type-id is int or not
    try:
        type_id = int(type_id)
    except:
        return jsonify({"status": "invalid form data", "error": "type id is not int"}), 400

    ''' after verifying everything is correct '''

    ''' read the file and see if the necessary data is provided
        if not then exit the function 
    '''

    mandatory_fields = await mongo_catalogdb.Fetch.attributes(type_id).mandatory()  
    all_fields = await mongo_catalogdb.Fetch.attributes(type_id).all()

    niche_specific_fields = await mongo_catalogdb.Fetch.attributes(type_id).niche_specific()

    sheet = await asyncio.to_thread(sheets.read_xlsx, xlsx_sheet)

    new_sheet = None
    brand_name = await branddb.Fetch.brand_name_by_id(session.get('brand'))
    error_encountered = False
    for iteration, document in enumerate(sheet):

        '''only check once if headers are tempered or not'''
        if iteration == 0:
            document_header = [key for key in document.keys()]
            if Counter(document_header) != Counter(all_fields):
                return jsonify({"status": "failed", "msg": "redownload the bulk upload file and re-upload"}), 422

        ''' in case user didn't give the vendor name then brand by default is the brand '''
        if document.get('vendor') == None:
            document["vendor"] = brand_name

        '''validate the document whether all the required fields are given or not'''
        valid_paylaod = helper.Helper.check_required_payload(document, all_fields, mandatory_fields)
    
        if valid_paylaod == True:
            usku_id = await products.create_usku()

            sql_catalog_data = {key: document.get(key) for key in document if key not in niche_specific_fields}

            '''adding the necessary ids to the sql catalog data'''
            sql_catalog_data["usku_id"] = usku_id
            sql_catalog_data["brand_id"] = session.get("brand")
            sql_catalog_data["type_id"] = type_id

            mongo_catalog_data = {key: document.get(key) for key in document if key in niche_specific_fields}
            mongo_catalog_data["type_id"] = type_id

            response = await catalogdb.Write.catalog(sql_catalog_data)
            
            if response == "ok":
                if new_sheet == None:
                    new_sheet = xlsx_sheet

                await mongo_catalogdb.Write.single_catalog(mongo_catalog_data)
                new_sheet = await asyncio.to_thread(sheets.remove_row, new_sheet, iteration+2) # iteration starts from 0 and gives first row so he have to add 1
            else:
                if response.get("error") == 1062:
                    return jsonify({"status": "failed", "msg": f"duplicate Sku id at row {iteration+2}"}), 409
                error_encountered = True
        else:
            error_encountered = True

    '''return the sheet containing the data which could not be uploaded due to mandatory data not being available'''
    if error_encountered == True and new_sheet != None:
        return Response(new_sheet), 422
    elif error_encountered == True and new_sheet == None: # which means it didn't even upload any
        return jsonify({"status": "failed", "msg": "check the whether you have filled the mandatory fields"}), 422
    return jsonify({"status": "ok"}), 200



# get the xlsx sheet for bulk upload
@catalog.get('/catalog/bulk-excel-sheet')
@login_required
@brand_required
async def get_bulk_upload_sheet():
    product_type_id = request.args.get('type')

    # checking whether the id is int or not
    try:
        product_type_id = int(product_type_id)
    except Exception:
        return jsonify({"status": "invalid id", "msg": "id should be an integer"}), 400

    headers = await mongo_catalogdb.Fetch.attributes(product_type_id).all()
    mandatory_fields = await mongo_catalogdb.Fetch.attributes(product_type_id).mandatory()
    sheet = await asyncio.to_thread(sheets.create_xlsx, headers, mandatory_fields)
    return  Response(sheet)



# some data are niche specific soo for the front end to show them, it has to fetch it first
# this route will provide the data fields which for niche specific attributes
@catalog.get('/catalog/attribute-fields')
@login_required
@brand_required
async def get_attribute_fields():

    niche_id = request.args.get('type')

    #sanitising the arguments
    if niche_id is None:
        return jsonify({'status': "invalid argument", "msg": "no niche field available, it should be ?niche=<id>"}), 400
    else:
        try:
            niche_id = int(niche_id)
        except Exception as e:
            return jsonify({"staus": "invalid value", "msg": "the id should be int type"})

    
    niche_attributes = await mongo_catalogdb.Fetch.catalog_schema(niche_id)
    image_attributes = await mongo_catalogdb.Fetch.image_schema(niche_id)

    if niche_attributes.get('error') is not None:
        return jsonify({"status": "interrupted", "msg": "attributes are not available for this product"}), 500
    
    return jsonify({
        "field_attributes": niche_attributes,
        "image_attributes": image_attributes
    })



'''
    HANDLING THE IMAGE
'''

@catalog.post("/catalog/image")
@login_required
@brand_required
async def upload_image():
    args = request.args
    usku_id = args.get("usku-id", type=str)
    order = args.get("order", default=-1, type=int)
    image_type = args.get("image-type", default="front", type=str)

    '''checking if the usku_id is correct'''
    is_usku_exists = await catalogdb.Fetch.is_usku_id_exists(usku_id)

    if is_usku_exists != True:
        return jsonify({"status": "invalid usku_id", "msg": 'usku id does not exists'})
    
    if order < 0:
        return jsonify({"status": "request failed", "error": "invalid value for order in argument"}), 409

    file = await request.files
    image_file = file.get("image")

    '''checking the file type'''
    check_image = image_file.filename.endswith((".png", ".webp", ".jpeg", ".jpg"))

    if check_image is False:
        return jsonify({"status": "failed", "msg": "file type should be an image"}), 415
    
    '''store the original image to .product_images/.original_images'''

    image_extended_filename = image_file.filename.split(".")
    image_extension = image_extended_filename[len(image_extended_filename)-1]
    
    original_image_name = f"{usku_id}_-_{image_type}.{image_extension}"
    webp_image_name = f"{usku_id}_-_{image_type}.webp"

    image = image_file.read()
    
    '''adding image entry into the databases'''
    img_object = {
        "usku_id": usku_id,
        "url": {"original" :f"/catalog/original_image/{original_image_name}",
                "high_resol_webp": f"/catalog/high_resol_webp/{webp_image_name}",
                "low_resol_webp": f"/catalog/low_resol_webp/{webp_image_name}",
                "webp_card": f"/catalog/webp_card/{webp_image_name}",
                },
        "type": image_type,
        "order": order 
    }

    write_buffer_size = current_app.config["IMAGE_WRITE_BUFFER"]
    result = await asyncio.gather(imageio.write(image, original_image_name, write_buffer_size), 
                                  catalogdb.Write.image(img_object))

    if result[0] == "error" or result[1] != "ok":
        return jsonify({"status": "failed", "msg": "issue occured while uploading the image"}), 500
    
    return jsonify("ok")


@catalog.get("/catalog/image")
@login_required
@brand_required
async def get_product_image():
    arguments = request.args

    usku_id = arguments.get("usku-id")
    type = arguments.get("image-type")

    if usku_id == None:
        return jsonify({"status": "failed", "msg": "usku id is not provided"}), 409
    elif type == None:
        return jsonify({"status": "failed", "msg": "image type is is not provided"}), 409

    '''checking the usku_id'''
    if not await catalogdb.Fetch.is_usku_id_exists(usku_id):
        return jsonify({"status": "failed", "msg": "invalid usku-id"}), 409

    image_urls = await catalogdb.Fetch.image(usku_id, type)

    if image_url == "error":
        return jsonify({"status": "failed", "msg": "could not finish the request"}), 500
    elif image_url == None:
        return jsonify({"status": "failed", "msg": "invalid image type"}), 409

    return jsonify(image_urls)


@catalog.get("/catalog/<image_variant>/<filename>")
async def image_url(image_variant: str, filename: str):
    buffer_size = current_app.config["IMAGE_READ_BUFFER"]

    mimetype = "image/webp"
    if image_variant == "webp_card":
        filename = f"./.product_images/.image_cards/{filename}"
    elif image_variant == "original_image":
        split_name = filename.split(".")
        extension = split_name[len(split_name)-1]
        mimetype = f"image/{extension}"
        filename = f"./.product_images/.original_images/{filename}"
        
    elif image_variant == "high_resol_webp":
        filename = f"./.product_images/.high_resol_images/{filename}"
    elif image_variant == "low_resol_webp":
        filename = f"./.product_images/.low_resol_images/{filename}"
    else:
        abort(404)

    image = imageio.read_image_card(filename, buffer_size)
    
    if image is None:
        abort(404)
    
    return Response(image, mimetype=mimetype), 200