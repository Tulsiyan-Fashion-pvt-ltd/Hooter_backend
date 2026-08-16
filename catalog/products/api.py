from quart import Blueprint, session, request, jsonify, Response, current_app, abort, json, make_response
from catalog.products import mariadb
from brand.auth import mariadb as brand_sql
from catalog.categories import mongodb as categories
from catalog.products import mongodb 
from catalog.products.utils.id import create_usku, create_variant_id
from utils.prerequirements import login_required, brand_required
from utils import helper
from utils import workbook
# from utils import imageio delete image logic needs to be worked on
from utils.helper import Payload
import asyncio
from catalog.providers.shopify import products as shopify_products
from config import _platforms
from . import services
from utils.custom_response import make_multipart_response


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
    usku_id = await services.Products.create(listing_attributes = listing_attributes,
                                   category_attributes = product_attributes,
                                   category_id = category_id
                                )

    if type(usku_id) != str:
        return jsonify({"status": "failed", "message": usku_id.get("error")}), usku_id.get("code")

    return jsonify({"status": "successful", "message": "added the single catalog", "usku_id": usku_id}), 200


"""UNDER CONSTRUCTION 🚧🚧🚧"""
# @products.post("/variants/<usku_id>")
# @login_required
# @brand_required
# async def upload_variants(usku_id: str):
#     variants = await request.get_json()

#     db = await services.Variants.create(usku_id, variants)
#     if db.get("error"):
#         return jsonify({"status": "failed", "message": db.get("error")}), 400

#     return jsonify({"status": "successful", "message": db.get("message")}), 200
    


# upload bulk catalog to the hooter backend
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

    """iterating over the document and pushing in the system after verification"""
    sheet = await asyncio.to_thread(workbook.read_generator, xlsx_sheet)

    new_sheet = None # new sheet for the un-uploaded files
    brand_name = await brand_sql.Fetch.brand_name_by_id(session.get('brand'))
    for iteration, document in enumerate(sheet):

        '''only check once if headers are tempered or not'''
        if iteration == 0:
            document_header = document.keys()

            if not set(expected_mandatory_keys).issubset(set(document_header)): # if mandatory keys exists
                return jsonify({"status": "failed", "msg": "redownload the bulk upload file and re-upload"}), 422

        ''' in case user didn't give the vendor name then brand by default is the brand '''
        if document.get('vendor') == None:
            document["vendor"] = brand_name

        '''validate the document whether all the required fields are given or not'''
        valid_paylaod = helper.Payload.check_required_payload(document, expected_mandatory_keys)
    
        if valid_paylaod == True:
            usku_id = create_usku()

            sql_catalog_data = {key: document.get(key) for key in document if key in catalog_all_keys}

            '''adding the necessary ids to the sql catalog data'''
            sql_catalog_data["usku_id"] = usku_id
            sql_catalog_data["brand_id"] = session.get("brand")
            sql_catalog_data["type_id"] = type_id


            mongodb_catalog_data = {key: document.get(key) for key in document if key not in catalog_all_keys}
            mongodb_catalog_data["usku_id"] = usku_id

            """Making bulk payload"""
            response = await mariadb.Write.product(sql_catalog_data)    

            if not response.get("error"):
                asyncio.create_task(mongodb.Write.product(mongodb_catalog_data))
                # new_sheet = await asyncio.to_thread(workbook.remove_row, new_sheet, iteration+2) # iteration starts from 0 and gives first row so he have to add 1
            else:
                error = None
                if new_sheet == None:
                    fields = workbook.read_row(xlsx_sheet, index=1)
                    names = workbook.read_row(xlsx_sheet, index=2)

                    #creating new sheet with an extra error column
                    new_sheet = workbook.create(fields=fields, names = ["Error"] +  names)
                    if not new_sheet:
                        return jsonify({"status": "failed", "message": "upload failed"}), 500

                if response.get("error") == 1062:
                    error = "duplicate sku id"
                elif response.get("error") == 1048:
                    error = "duplicate sku id"
                else:
                    error = "upload failed"       

                row = workbook.read_row(xlsx_sheet, index=iteration+3) # value starts from row 3
                new_sheet = workbook.write(new_sheet, row=[error] + row)
        else:
            """"""
            if new_sheet == None:
                fields = workbook.read_row(xlsx_sheet, index=1)
                names = workbook.read_row(xlsx_sheet, index=2)
                
                new_sheet = workbook.create(fields=fields, names = ["Error"] + names)
                if not new_sheet:
                    return jsonify({"status": "failed", "message": "upload failed"}), 500

            row = workbook.read_row(xlsx_sheet, index=iteration+3) # value starts from row 3
            new_sheet = workbook.write(new_sheet, row = ["mandatory fields can not be empty"] + row)

    '''return the sheet containing the data which could not be uploaded due to mandatory data not being available'''

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
        return jsonify({"status": "failed", "msg": "request failed"}), 400
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
        return jsonify({"status": "request failed", "msg": "could not fetch the catalog data"}), 500
    
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
                root_path = current_app.root_path

                if image_type == "webp_card":
                    file_path = f"{root_path}/.product_images/.image_cards/{filename}"
                elif image_type == "original":
                    file_path = f"{root_path}/.product_images/.original_images/{filename}"    
                elif image_type == "high_resol_webp":
                    file_path = f"{root_path}/.product_images/.high_resol_images/{filename}"
                elif image_type == "low_resol_webp":
                    file_path = f"{root_path}/.product_images/.low_resol_images/{filename}"

                # tasks.append(imageio.delete_image(file_path))                  
                
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            return jsonify({"status": "successful", "msg": "image deletion scheduled"}), 202       
        
        return jsonify({"status": "successful", "msg": "item deleted from the catalog"}), 200   


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
        return jsonify({"status": "failed", "msg": "invalid payload"}), 400

    '''checking the payload'''
    payload_list = await asyncio.gather(categories.Fetch.attributes(type_id).all(),
                                  categories.Fetch.attributes(type_id).mandatory())
    
    accepted_payload = payload_list[0]
    mandatory_payload = payload_list[1]

    
    accepted_payload.append("discount")

    if not helper.Helper.check_required_payload(data, accepted_payload, mandatory_payload):
        return jsonify({"status": "failed", "msg": "invalid payload"}), 400
    
    brand_name = await mariadb.Fetch.brand_name_by_id(session.get('brand'))

    # data for sql
    catalog = {
        "brand_id": session.get('brand'),
        "usku_id": usku_id,
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
                    if (key not in catalog)}

    mongodb["usku_id"] = usku_id
    mongodb["type_id"] = type_id  

    response = await asyncio.gather(mariadb.Write.update_catalog(catalog), 
                                    mongodb.Write.update_catalog(mongodb_catalog))
    
    if response[0] != "ok" or response[1] != "ok": 
        return jsonify({"status": "failed", "msg": "error occured while updating the catalog"}), 500
    
    return jsonify({"status": "successful", "msg": "updated successfully"}), 200


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
            return jsonify({"status": "successful", "msg": "updated the catalog upload as completed"}), 200
        else:
            jsonify({"status": "failed", "msg": "error encountered while updating the status as completed"}), 500
    else:
        return jsonify({"status": "failed request", "msg": "usku_id does not exists"}), 400               
    return jsonify({"status": "request completed", "msg": "reqeust completed without updating the status"}), 202



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