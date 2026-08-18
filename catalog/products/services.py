from werkzeug.datastructures import FileStorage
from catalog.products import mongodb 
from catalog.products import mariadb
from catalog.products.utils.id import create_variant_id, create_usku
from brand.auth import mariadb as brand_sql
from catalog.images import mariadb as imagesql
import asyncio
from quart import session
from config import _product_image_bucket
import json
from traceback import print_exc
from utils import helper, workbook
import s3
from io import BytesIO


async def create_variants(usku_id: str, variants: list) -> dict| str:
    """
    RETURNS:\n
    If the operation is successful then "ok"\n
    else\n
    {"error": "error message"}
    """
    for variant in variants:
        variant["variant_id"] = create_variant_id(usku_id)

    db = await mariadb.Write.variants(usku_id, variants)
    if db.get("error") is not None:
        if db.get("error") == 1452:
            return {"error": "USKU ID does not exists"}
        return {"error": "Could not create the variants"}
    
    else:
        return "ok"




async def create_product(listing_attributes: dict, category_attributes: dict, category_id: str) -> dict| str:
    """
    RETURNS:\n
    If the operation is successful then `{"status": "successful", "message": "product upload completed", "usku_id": usku_id, "code": 200}`\n
    else\n
    `{"error": "error message"}`
    """
    
    usku_id = create_usku()
    brand_name = await brand_sql.Fetch.brand_name_by_id(session.get("brand"))

    ## ADDING THE THE DATA IN THE SQL
    sql_attributes_value = {
    "brand_id": session.get("brand"),
    "usku_id": usku_id,                     
    "type_id": category_id,
    "vendor": listing_attributes.get("vendor", brand_name),          
    "brand_name": listing_attributes.get("brand_name", brand_name),
    }

    mongodb_attribute_value = {
        "usku_id": usku_id
    }

    #updating the values
    listing_attributes.update(sql_attributes_value)
    category_attributes.update(mongodb_attribute_value)

    response = await mariadb.Write.product(listing_attributes) # db query
    if response.get("error") is not None:
        if response.get('error') == 1062:
            return {"error": "Duplicate sku id", "code": 409}
        
        elif response.get("error") == 1366:
            return {"error": "Incorrect value for the listing_attributes fields", "code": 400}
        
    # add the details in the mongodb db
    mongo_response = await mongodb.Write.single_catalog(category_attributes)

    if mongo_response.get('error'):
        return {"status": "successful", "message": "Product has listed but the product data could not be uploaded", "code": 202}
    return {"status": "successful", "message": "product upload completed", "usku_id": usku_id, "code": 200}




async def update_product(usku_id: str, data: dict, type_id: str) -> tuple[dict[str, str], int]:
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

    response = await asyncio.gather(mariadb.Write.update_catalog(catalog), 
                                    mongodb.Write.update_catalog(mongodb_catalog))
    
    if response[0] != "ok" or response[1] != "ok": 
        return {"status": "failed", "message": "error occured while updating the catalog"}, 500

    return {"status": "successful", "message": "successfully updated the catalog product"}, 200



async def delete_product(usku_id: str) -> tuple[dict[str, str], int]:
    """Deletes the product from rdbms(mariadb), nosql(mongodb) and then images from s3 server(minio)
    
    Parameters:
        usku_id:

    Returns:
        returns the `tuple()` with 
        dict:
            at 

    """
    images = await imagesql.Fetch.image(usku_id) # image object keys for s3
    print(images)
    
    db_query = await asyncio.gather(mariadb.Write.delete_catalog(usku_id), 
                                    mongodb.Write.delete_catalog(usku_id)
                                    )
    
    if db_query[0] != "ok" or db_query[1] != "ok" or images == "error":
        return {"status": "request failed", "message": "error occured while deleting from the catalog"}, 500
    else:
        # print(images)
        # todo
        '''iterate over each image names and sepeate with their file paths acc to the image type
            and send to the image delete function from imageio
        '''
        tasks = []
        for image_details in images:
            '''we have deleted the image records from the dbms so we are calling the s3 bucket 
            rather the delete image service from the catalog.images'''
            tasks.append(asyncio.to_thread(s3.delete_bulk_objects, _product_image_bucket, keys=json.loads(image_details.get("image_url")).values()))
        try:
            s3_response = await asyncio.gather(*tasks)
            if "error" in s3_response:
                return {"status": "successful", "message": "could not delete the product images but they are inaccessible"}, 202
        except Exception as e:
            print(f"could not proceed to delete the images in the product delete api")
            print_exc()
            print(e)
            return {"status": "successful", "message": "image deletion scheduled"}, 202       
        
        return {"status": "successful", "message": "item deleted from the catalog"}, 200




async def upload_xlsx(xlsx_sheet: FileStorage, category_id: str, all_listing_attributes: list, product_mandatory_attributes: list):
    """Upload bulk products to the server and create a new xlsx sheet if the upload has any error in it.
    It checks the product payload if it the mendatory fields are provided and then lists the product on sql and 
    runs a fire-and-forget function to upload product data on mongodb
    
    Parameters:
        xlsx_sheet:
            upload sheet
        category_id:
            category id from product categories taxonomy
        all_listing_attributes:
            list of listing attributes for sql
        product_mandatory_attributes:
            mandatory fields of product (rdbms and mongodb mandatory attributes combined)

    
    Returns:
    """    

    sheet = await asyncio.to_thread(workbook.read_generator, xlsx_sheet)
    fields, names = await asyncio.gather(asyncio.to_thread(workbook.read_row(xlsx_sheet, row=1)),
                                             asyncio.to_thread(workbook.read_row(xlsx_sheet, row=2)))

    error_sheet = None
    tasks = []
    fields, names = [], []
    for attributes in await asyncio.to_thread(workbook.read_generator(sheet)):
        '''VALIDATING PRODUCT ATTRIBUTES'''
        values = [] # getting the values of each products
        listing_attributes, category_attributes = {}, {}

        #creating fields, names sql and mongodb attribute objects list out of attributes collection of attribute object
        for attribute in attributes:
            fields.append(attribute.get("field")) if len(fields) <= len(attributes) else None #logic if the len of fields is equal to lens of attributes which means any next append is duplicate
            names.append(attributes.get("name")) if len(names) <= len(attributes) else None

            '''POPULATING LISTING AND CATEGORY ATTRIBUTES FOR PRODUCT CREATION'''
            if attribute.get("field") in all_listing_attributes:
                listing_attributes[attribute.getattribute.get("field")] = attribute.getattribute.get("value") 
            else:
                category_attributes[attribute.getattribute.get("field")] = attribute.getattribute.get("value") 


        # the error message will go the new xlsx sheet with error column in the beginnining
        if not set(product_mandatory_attributes).issubset(set(fields)):
            if error_sheet is None:
                error_sheet = await asyncio.to_thread(workbook.create(fields=fields, names=["Error"]+names))

            await asyncio.to_thread(workbook.write(error_sheet, row=["Mandatory fields can not be empty"]+values))

        '''UPLOAD THE DATA'''
        response = await create_product(listing_attributes, category_attributes, category_id)
        if response.get('error'):
            if error_sheet is None:
                error_sheet = await asyncio.to_thread(workbook.create(fields=fields, names=["Error"]+names))

            await asyncio.to_thread(workbook.write(error_sheet, row=[response.get('error')]+values))