from werkzeug.datastructures import FileStorage
from catalog.products import mongodb
from catalog.categories import mongodb as categoriessql 
from catalog.products import mariadb
from catalog.categories import mongodb as category_mongodb
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
from . import services


'''tasks STORES THE OBJECT WITH KEYS job_id: {}, 
WITH THEIR KEYS "status: pending| failed| completed", 
"event: async.Event() supports .wait() and .set() and .clear()", "task: the task pointer from create_task()"'''
tasks = {} # object to store the tasks


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
    If the operation is successful then 
        - `{"status": "successful", "message": "product upload completed", "usku_id": usku_id, "code": 200}`\n
    else\n
        - `{"error": "error message"}`
    """
    '''CHECKIGN CATEGORY ID'''
    category_mongo_request = await category_mongodb.Fetch.category_name(category_id)
    type_name = category_mongo_request.get('name')
    if not type_name:
        return {"error": "invalid request", "message": "type id is incorrect", "code": 400}
    
    usku_id = create_usku()
    brand_name = await brand_sql.Fetch.brand_name_by_id(session.get("brand"))

    '''ADDING THE THE DATA IN THE DB AFTER FORMATTING IT'''
    sql_attributes_value = {
    "brand_id": session.get("brand"),
    "usku_id": usku_id,                     
    "type_id": category_id,
    "vendor": listing_attributes.get("vendor", brand_name),          
    "brand_name": listing_attributes.get("brand_name", brand_name),
    "type_name": type_name
    }

    mongodb_attribute_value = {
        "usku_id": usku_id
    }

    #updating the values
    listing_attributes.update(sql_attributes_value)
    category_attributes.update(mongodb_attribute_value)

    try:
        sql_response, mongo_response = await asyncio.gather(mariadb.Write.product(listing_attributes), 
                                        mongodb.Write.product(category_attributes)) # db query
        if sql_response.get("error") is not None:
            if sql_response.get('error') == 1062:
                return {"error": "Duplicate sku id", "code": 409}

            elif sql_response.get("error") == 1366:
                return {"error": "Incorrect value for the listing_attributes fields", "code": 400}


        if mongo_response.get('error'):
            return {"status": "successful", "message": "Product has listed but the product data could not be uploaded", "code": 202}
        return {"status": "successful", "message": "product upload completed", "usku_id": usku_id, "code": 200}
    except Exception as e:
        print(e)
        print_exc()
        return {"status": "failed", "error": "Error occured while uploading the product", "code": 500}




async def update_product(usku_id: str, listing_attributes: dict, category_attributes: dict) -> tuple[dict[str, str], int]:
    response = await asyncio.gather(mariadb.Write.update_catalog(usku_id, listing_attributes), 
                                    mongodb.Write.update_catalog(usku_id, category_attributes))
    
    if response[0] != "ok" or response[1] != "ok": 
        return {"status": "failed", "message": "error occured while updating the catalog"}, 500

    return {"status": "successful", "message": "successfully updated the catalog product"}, 200




async def delete_product(usku_id: str) -> tuple[dict[str, str], int]:
    """Deletes the product from rdbms(mariadb), nosql(mongodb) and then images from s3 server(minio)
    
    Parameters:
        usku_id:

    Returns:
        returns the `tuple()` with 
        - dict:
        - api status

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
        for image_details in images if images else []:
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




async def upload_xlsx(xlsx_data: bytes, category_id: str) -> dict[str, str|int|BytesIO]:
    """Upload bulk products to the server and create a new xlsx sheet if the upload has any error in it.
    It checks the product payload if it the mendatory fields are provided and then lists the product on sql and 
    runs a fire-and-forget function to upload product data on mongodb
    
    Parameters:
        xlsx_data:
            upload sheet's BytesIO object so the references outlives the request
        category_id:
            category id from product categories taxonomy

    
    Returns:
        dict:
            on successful upload:
            - `"status" : "successful"` 
            - `"message": str` 
            - `"code": 200`

            on partial uploads:
            - `"sheet": BytesIO(xlsx sheet)`
            - `"code": 202`

    """    
    try:
        '''CHECKING ANY CORRUPTION IN HEADER VALUES'''
        all_listing_attributes, listing_mandatory_keys, category_mandatory_keys = await asyncio.gather(categoriessql.Fetch.Attributes.Catalog.all(),
                                                                                                categoriessql.Fetch.Attributes.Catalog.mandatory(), 
                                                                                                 categoriessql.Fetch.Attributes.Category(category_id).mandatory())
        product_mandatory_attributes  = listing_mandatory_keys + category_mandatory_keys
    
    
        '''HEADER VERIFICATION'''
        xlsx_sheet = BytesIO(xlsx_data)
        document_header = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=1) # it's putting the pointer after the first row
        if not set(product_mandatory_attributes).issubset(set(document_header)): # if mandatory keys exists
            return {"status": "failed", "message": "redownload the bulk upload file and re-upload"}

        fields = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=1)
        names = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=2)

        error_sheet = None
        fields, names = [], []

        '''Iterating over sheet rows'''
        for attributes in await asyncio.to_thread(workbook.read_generator, BytesIO(xlsx_data)): # creating the fresh sheet to void any pointer conflicts
            '''VALIDATING PRODUCT ATTRIBUTES'''
            values = [] # getting the values of each products
            listing_attributes_object, category_attributes_object = {}, {}

            #creating sql and mongodb attribute objects list out of attributes collection of attribute object
            for attribute in attributes:
                fields.append(attribute.get("field")) if len(fields) <= len(attributes) else None #logic if the len of fields is equal to lens of attributes which means any next append is duplicate
                names.append(attribute.get("name")) if len(names) <= len(attributes) else None
                values.append(attribute.get("value")) # values is defined inside the loop so it's getting cleared

                '''POPULATING LISTING AND CATEGORY ATTRIBUTES FOR PRODUCT CREATION'''
                if attribute.get("field") in all_listing_attributes:
                    listing_attributes_object[attribute.get("field")] = attribute.get("value") 
                else:
                    category_attributes_object[attribute.get("field")] = attribute.get("value") 

            # print(values)
            '''IF ANY OF THE MANDATORY ATTRIBUTE VALUE IS NULL'''
            if not (helper.Payload.check_required_payload(listing_attributes_object, listing_mandatory_keys)
                and helper.Payload.check_required_payload(category_attributes_object, category_mandatory_keys)):

                try:
                    if error_sheet is None:
                        error_sheet = await asyncio.to_thread(workbook.create, fields=['error']+ fields, names=["Error"]+names)

                    error_sheet = await asyncio.to_thread(workbook.write, error_sheet, row=["Mandatory fields can not be empty"]+values)
 
                except Exception as e:
                    print(e)
                    print_exc()
                    return {"error": "unable to create error sheet"}

            else:
                '''UPLOAD THE DATA'''
                try:
                    response = await create_product(listing_attributes_object, category_attributes_object, category_id)
                    if response.get('error'):
                        if error_sheet is None:
                            error_sheet = await asyncio.to_thread(workbook.create, fields=['error']+ fields, names=["Error"]+names)

                        error_sheet = await asyncio.to_thread(workbook.write, error_sheet, row=[response.get('error')]+values)
                except Exception as e:
                    print(e)
                    print_exc()
                    return {"status": "failed", "error": "unable to upload product or create error sheet"}
        print("finished")
        if not error_sheet:
            return {"status": "successful", "message": "Uploaded the products", "code": 200}
        else: 
            error_sheet = await asyncio.to_thread(workbook.write, error_sheet, row=["Remove the error column entirely before uploading this file after correction"])
            return {"sheet": error_sheet, "code": 202}
    except Exception as e:
        print(e)
        print_exc()
        return {"error": "could finish uploading products"}




def on_task_complete(task):
    job_id = task.job_id

    if not job_id:
        raise Exception("no job_id assigned to the task object")

    if task.exception():
        status = "failed"
    else:
        status = "completed"

    if job_id in tasks:
        tasks[job_id]["status"] = status
        tasks[job_id]["event"].set() # set the event flag as True
        # this will release the sse