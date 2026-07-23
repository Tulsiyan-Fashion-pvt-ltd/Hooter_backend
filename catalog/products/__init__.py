from quart import Blueprint, session, request, jsonify, Response, current_app, abort, json
from catalog.products import mariadb
from catalog.services import products
from utils.prerequirements import login_required, brand_required
from catalog.categories import mongodb as categories
from catalog.products import mongodb 
from utils import helper
from utils import sheets
from utils import imageio
from . import mongodb
import asyncio
from collections import Counter


procucts = Blueprint("products", __name__, url_prefix = "/products")


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
@products.post('/single-catalog')
@login_required
@brand_required
async def upload_single_catalog():

    payload = await request.get_json()

    accepted_main_keys = ["type", "data"]
    if not helper.Helper.check_required_payload(payload, accepted_main_keys, accepted_main_keys):
        return jsonify({"status": "invalid payload", "missing keys": accepted_main_keys}), 422

    data = payload.get('data')
    niche_type = payload.get('type')

    try:
        niche_type = int(niche_type)
    except:
        return jsonify({"status": "invalid argument"}), 400

    #checking the payload 
    system_keys = await asyncio.gather(categories.Fetch.attributes(niche_type).all(),
                                       categories.Fetch.attributes(niche_type).mandatory())
    accepted_data_keys = system_keys[0]
    necessary_data_keys = system_keys[1]

    if not helper.Helper.check_required_payload(data, accepted_data_keys, necessary_data_keys):
        return jsonify({"status": "invalid payload", "accepted_keys": accepted_data_keys, "mandatory": necessary_data_keys}), 400

    
    ## ADDING THE THE DATA IN THE SQL
    brand_name = await mariadb.Fetch.brand_name_by_id(session.get('brand'))

    catalog = {
    "brand_id": session.get('brand'),
    "usku_id": await products.create_usku(),
    "sku_id": data.get("sku_id"),                          # TEMP FIX: was "sku-id"
    "type_id": niche_type,
    "product_title": data.get('product_title'),
    "price": data.get("price"),
    "compared_price": data.get("compared_price"),          # TEMP FIX: was "compared-price" (key + get)
    "purchasing_cost": data.get("purchasing_cost"),        # TEMP FIX: was "purchasing-cost"
    "vendor": data.get("vendor") if data.get("vendor") else brand_name,
    "ean": data.get('ean'),
    "hsn": data.get("hsn"),
    "net_weight_kg": data.get("net_weight_kg"),                  # TEMP FIX: was "net-weight"
    "dead_weight_kg": data.get("dead_weight_kg"),                # TEMP FIX: was "dead-weight"
    "volumetric_weight_kg": data.get("volumetric_weight_kg"),    # TEMP FIX: was "volumentric_weight" + "volumetric-weight" (typo + hyphen)
    "brand_name": data.get("brand_name") if data.get("brand_name") else brand_name  # TEMP FIX: was "brand-name"
}

    response = await mariadb.Write.catalog(catalog)

    if response != "ok":
        if response.get('error') == 1062:
            return jsonify({"status": "failed", "error": "duplicate sku id"}), 409
        else:
            return jsonify({"error encountered while adding the catalog"}), 500
    

    # add the details in the mongodb db
    mongodb_catalog_data = {"type_id": niche_type, "usku_id": catalog.get("usku_id")}
    niche_specific_keys = await categories.Fetch.attributes(niche_type).niche_specific()
    for key in niche_specific_keys:
        mongodb_catalog_data[key] = data.get(key)

    await mongodb.Write.single_catalog(mongodb_catalog_data)
    return jsonify({"Status": "successful", "message": "added the single catalog", "usku_id": catalog.get("usku_id")}), 200



# upload bulk catalog to the hooter backend
@products.post('/bulk-catalog')
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

    mandatory_fields = await categories.Fetch.attributes(type_id).mandatory()  
    all_fields = await categories.Fetch.attributes(type_id).all()

    niche_specific_fields = await categories.Fetch.attributes(type_id).niche_specific()

    sheet = await asyncio.to_thread(sheets.read_xlsx, xlsx_sheet)

    new_sheet = None
    brand_name = await mariadb.Fetch.brand_name_by_id(session.get('brand'))
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

            mongodb_catalog_data = {key: document.get(key) for key in document if key in niche_specific_fields}
            mongodb_catalog_data["type_id"] = type_id
            mongodb_catalog_data["usku_id"] = usku_id

            response = await mariadb.Write.catalog(sql_catalog_data)
            
            if response == "ok":
                if new_sheet == None:
                    new_sheet = xlsx_sheet

                await mongodb.Write.single_catalog(mongodb_catalog_data)
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
@products.get('/bulk-excel-sheet')
@login_required
@brand_required
async def get_bulk_upload_sheet():
    product_type_id = request.args.get('type')

    # checking whether the id is int or not
    try:
        product_type_id = int(product_type_id)
    except Exception:
        return jsonify({"status": "invalid id", "msg": "id should be an integer"}), 400

    headers = await categories.Fetch.attributes(product_type_id).all()
    mandatory_fields = await categories.Fetch.attributes(product_type_id).mandatory()
    sheet = await asyncio.to_thread(sheets.create_xlsx, headers, mandatory_fields)
    return  Response(sheet)



# get the uploaded catalog products and status
@products.get("")
@login_required
@brand_required
async def list_catalog():
    args = request.args
    usku_id = args.get('usku-id')

    if usku_id:
        product_data = await asyncio.gather(mariadb.Fetch.catalog_product(usku_id),
                                            mongodb.Fetch.catalog_product(usku_id))
        
        if product_data[0].get("error") or product_data[1].get("error"):
            return jsonify({"status": "failed", "msg": "request failed"}), 500
        else:
            # print(product_data[1])
            product_data = product_data[0] | product_data[1]
            return jsonify(product_data), 200
    

    brand_id = session.get("brand")

    catalog_data = await asyncio.gather(mariadb.Fetch.catalog_upload_count(brand_id), 
                          mariadb.Fetch.catalog_list(brand_id))
    
    if catalog_data[0] == "error" or catalog_data[1] == "error":
        return jsonify({"status": "request failed", "msg": "could not fetch the catalog data"}), 500
    
    return jsonify({"count": catalog_data[0], "catalog-list": catalog_data[1]}), 200



'''
    this route serves the resource to delete a product from the catalog
'''
@products.delete("")
@login_required
@brand_required
async def delete_product():
    args = request.args
    usku_id = args.get("usku-id")
    # print(usku_id)
    if not usku_id:
        return jsonify({"status": "invalid request", "msg": "usku-id not provided"}), 400
    
    images = await mariadb.Fetch.image(usku_id)
    
    db_query = await asyncio.gather(mariadb.Write.delete_catalog(usku_id), 
                                    mongodb.Write.delete_catalog(usku_id)
                                    )
    
    if db_query[0] != "ok" or db_query[1] != "ok" or images == "error": 
        return jsonify({"status": "request failed", "msg": "error occured while deleting from the catalog"}), 500
    else:
        # print(images)
        # todo
        '''iterate over each image names and sepeate with their file paths acc to the image type
            and send to the image delete function from imageio
        '''

        tasks = []
        for image_details in images:
            for image_type, url in json.loads(image_details.get("image_url")).items():
                url_split = url.split("/")
                filename = url_split[len(url_split) -1]
                file_path = ''

                if image_type == "webp_card":
                    file_path = f"./.product_images/.image_cards/{filename}"
                elif image_type == "original":
                    file_path = f"./.product_images/.original_images/{filename}"    
                elif image_type == "high_resol_webp":
                    file_path = f"./.product_images/.high_resol_images/{filename}"
                elif image_type == "low_resol_webp":
                    file_path = f"./.product_images/.low_resol_images/{filename}"

                tasks.append(imageio.delete_image(file_path))                  
                
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            return jsonify({"status": "successful", "msg": "image deletion scheduled"}), 202       
        
        return jsonify({"status": "successful", "msg": "item deleted from the catalog"}), 200   


'''route to update the catalog'''
@products.put("")
@login_required
@brand_required
async def update_catalog_data():
    payload = await request.get_json()

    type_id = payload.get("type")
    data = payload.get("data")

    if type_id == None or data == None:
        return jsonify({"status": "failed", "msg": "invalid payload"}), 400

    '''checking the payload'''
    payload_list = await asyncio.gather(categories.Fetch.attributes(type_id).all(),
                                  categories.Fetch.attributes(type_id).mandatory())
    
    accepted_payload = payload_list[0]
    mandatory_payload = payload_list[1]
    
    mandatory_payload.append("usku_id")
    accepted_payload.append("usku_id")
    accepted_payload.append("discount")

    # print(mandatory_payload)
    # print("\n")
    # print(accepted_payload)

    if not helper.Helper.check_required_payload(data, accepted_payload, mandatory_payload):
        return jsonify({"status": "failed", "msg": "invalid payload"}), 400
    
    brand_name = await mariadb.Fetch.brand_name_by_id(session.get('brand'))

    # data for sql
    catalog = {
        "brand_id": session.get('brand'),
        "usku_id": data.get("usku_id"),
        "sku_id": data.get("sku_id"),                          # TEMP FIX: was "sku-id"
        "type_id": type_id,
        "title": data.get('product_title'),
        "price": data.get("price"),
        "compared_price": data.get("compared_price"),          # TEMP FIX: was "compared-price" (key + get)
        "purchasing_cost": data.get("purchasing_cost"),        # TEMP FIX: was "purchasing-cost"
        "vendor": data.get("vendor") if data.get("vendor") else brand_name,
        "ean": data.get('ean'),
        "hsn": data.get("hsn"),
        "net_weight": data.get("net_weight_kg"),                  # TEMP FIX: was "net-weight"
        "dead_weight": data.get("dead_weight_kg"),                # TEMP FIX: was "dead-weight"
        "volumetric_weight": data.get("volumetric_weight_kg"),    # TEMP FIX: was "volumentric_weight" + "volumetric-weight" (typo + hyphen)
        "brand_name": data.get("brand_name") if data.get("brand_name") else brand_name  # TEMP FIX: was "brand-name"
    }

    #data for mongodbdb
    mongodb_catalog = {key: value for key, value in data.items() 
                    if (key not in catalog) or (key in ("usku_id", "type_id"))}


    response = await asyncio.gather(mariadb.Write.update_catalog(catalog), 
                                    mongodb.Write.update_catalog(mongodb_catalog))
    
    if response[0] != "ok" or response[1] != "ok": 
        return jsonify({"status": "failed", "msg": "error occured while updating the catalog"}), 500
    
    return jsonify({"status": "successful", "msg": "updated successfully"}), 200


# mark the catalog upload as completed
'''this function is meant to call after the images and catalog upload is successfull'''
@products.put("/mark-complete")
@login_required
@brand_required
async def mark_complete():
    args = request.args
    usku_id = args.get("usku-id")

    if usku_id and await mariadb.Fetch.is_usku_id_exists(usku_id):
        db_query = await mariadb.Write.status_complete(usku_id)
        if db_query == "ok":
            return jsonify({"status": "successful", "msg": "updated the catalog upload as completed"}), 200
        else:
            jsonify({"status": "failed", "msg": "error encountered while updating the status as completed"}), 500
    else:
        return jsonify({"status": "failed request", "msg": "usku_id does not exists"}), 400               
    return jsonify({"status": "request completed", "msg": "reqeust completed without updating the status"}), 202