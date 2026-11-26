import asyncio
from io import BytesIO
from catalog.categories import mongodb as categorie_ssql
from utils import helper, workbook
from time import time
from traceback import print_exc
from catalog.products.services import create_product
from catalog.categories.services import get_product_attributes
from . import models
from pydantic import ValidationError

async def upload_xlsx(ctx, xlsx_data: bytes, category_id: str, job_id=None) -> dict[str, str|int|BytesIO]:
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
        # listing_attributes = dict(payload.get('listing_attributes'))
        # product_attributes = dict(payload.get("category_attributes"))
        # product_attribute = await get_product_attributes(category_id, vertical=1)
    
        # try:
        #     ProductDetailsModel = models.buildProductAttributeModel(product_attribute[0].get('category_attributes'))
    
        #     listing_attributes = models.ListingAttributesModel.model_validate(listing_attributes).model_dump()
        #     product_attributes = ProductDetailsModel.model_validate(product_attributes).model_dump()
        # except ValueError as e:
        #     print('code: 24cca3b7', "error", e)
        #     print_exc()
        #     return {'status': 'failed', 'message': 'Invalid payload', 'code': '24cca3b7', 'error': e.errors()}, 400
        '''CHECKING ANY CORRUPTION IN HEADER VALUES'''
        all_listing_attributes, listing_mandatory_keys, category_mandatory_keys, product_attribute_schema = await asyncio.gather(categorie_ssql.Fetch.Attributes.Catalog.all(ctx=ctx),
                                                                                                categorie_ssql.Fetch.Attributes.Catalog.mandatory(ctx=ctx), 
                                                                                                categorie_ssql.Fetch.Attributes.Category(category_id).mandatory(ctx=ctx),
                                                                                                get_product_attributes(category_id, ctx=ctx))
        product_mandatory_attributes  = listing_mandatory_keys + category_mandatory_keys
        product_listing_schema = product_attribute_schema[0].get('listing_attributes')
        product_category_schema = product_attribute_schema[1].get('category_attributes')
    
        '''HEADER VERIFICATION'''
        xlsx_sheet = BytesIO(xlsx_data)
        document_header = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=1) # it's putting the pointer after the first row

        if not set(product_mandatory_attributes).issubset(set(document_header)): # if mandatory keys exists
            return {"status": "failed", "message": "redownload the bulk upload file and re-upload"}

        fields = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=1)
        names = await asyncio.to_thread(workbook.read_row, xlsx_sheet, row=2)

        '''values needs to be used multiple times so creating it outside the block and constructing once'''
        fields, names = [], []

        error_sheet = None # Storing eror sheet
        total_products = (await asyncio.to_thread(workbook.ttl_rows, xlsx_sheet)) - 2 #For progress report work_done/ttl_work, not counting header and name
        uploaded_products = 0
        init_time = time()

        '''Iterating over sheet rows'''
        for attributes in await asyncio.to_thread(workbook.read_generator, BytesIO(xlsx_data)): # creating the fresh sheet to void any pointer conflicts
            '''VALIDATING PRODUCT ATTRIBUTES'''
            values = [] # getting the values of each products
            listing_attributes, category_attributes = {}, {}

            #creating sql and mongodb attribute objects list out of attributes collection of attribute object
            for attribute in attributes:
                fields.append(attribute.get("field")) if len(fields) <= len(attributes) else None #logic if the len of fields is equal to lens of attributes which means any next append is duplicate
                names.append(attribute.get("name")) if len(names) <= len(attributes) else None
                values.append(attribute.get("value")) # values is defined inside the loop so it's getting cleared

                '''POPULATING LISTING AND CATEGORY ATTRIBUTES FOR PRODUCT CREATION'''
                if attribute.get("field") in all_listing_attributes:
                    listing_attributes[attribute.get("field")] = attribute.get("value") 
                else:
                    category_attributes[attribute.get("field")] = attribute.get("value") 

            '''PRODUCT ATTRIBUTES VALIDATION CHECK'''
            try:
                ProductDetailsModel = models.buildProductAttributeModel(product_category_schema)
                ListingAttributesModel = models.buildProductAttributeModel(product_listing_schema)

                listing_attributes = ListingAttributesModel.model_validate(listing_attributes).model_dump()
                product_attributes = ProductDetailsModel.model_validate(product_attributes).model_dump()
            except ValidationError as e:
                try:
                    if error_sheet is None:
                        error_sheet = await asyncio.to_thread(workbook.create, fields=['error']+ fields, names=["Error"]+names)

                    error_sheet = await asyncio.to_thread(workbook.write, error_sheet, row=[f"Field: {error.loc}, Error: {error.msg} " for error in e.errors(include_context=False)]+values)
 
                except Exception as e:
                    print(e)
                    print_exc()
                    return {"error": "unable to create error sheet"}

            else:
                '''UPLOAD THE DATA'''
                try:
                    response = await create_product(listing_attributes, category_attributes, category_id)
                    '''Counting progress for background job'''
                    uploaded_products += 1
                    # tasks[job_id]['progress'] = f"{round((uploaded_products/total_products)*100, 2)}%"
                    
                    if response.get('error'):
                        if error_sheet is None:
                            error_sheet = await asyncio.to_thread(workbook.create, fields=['error']+ fields, names=["Error"]+names)

                        error_sheet = await asyncio.to_thread(workbook.write, error_sheet, row=[response.get('error')]+values)
                except Exception as e:
                    print(e)
                    print_exc()
                    return {"status": "failed", "error": "unable to upload product or create error sheet"}

            # tasks.get(job_id).get('event').set() # turn the event flag false
            
        # tasks[job_id]["time"] = time() - init_time
        if not error_sheet:
            return {"status": "successful", "message": "Products uploaded", "code": 200}
        else: 
            error_sheet = await asyncio.to_thread(workbook.write, error_sheet, row=["Remove the error column entirely before uploading this file after correction"])
            return {"sheet": error_sheet, "code": 202}
    except Exception as e:
        print(e)
        print_exc()
        return {"error": "could finish uploading products"}