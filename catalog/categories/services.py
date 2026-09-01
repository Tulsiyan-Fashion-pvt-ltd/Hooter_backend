from quart import current_app
from catalog.categories import mongodb
import asyncio
from async_lru import alru_cache
from functools import wraps
from utils import workbook

def check_taxonomy(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if not current_app.taxonomy:
            raise Exception("taxonomy is not available")
        else:
            return await func(*args, **kwargs)
        
    return wrapper


@check_taxonomy
@alru_cache(maxsize=128, ttl=900)
async def get_taxonomy_attributes(vertical: int, id: str) -> list:
    """
    EXTRACT CATEGORY ATTRIBUTE FROM THE SHOPIFY TAXONOMY AND RETURNS IT.
    It is used in the categories so if the we don't have the attribute data in the db
        We can get it from the taxonomy and save it into the mongodb product_info_schema

    Parameters:
        - vertical
            the vertical index of the top level categories
        - id
            the id of the category

    Returns:
        Success:
            - list[dict[str, str]] -> list of dicts containing the attributes' information            
        On failure:
            - lsit[str, str] -> 
                list[0] -> `"error"` and list[0] -> error message
    """
    taxonomy = current_app.taxonomy
    name = None
    attributes = None
    for category in taxonomy[vertical].get("categories"):
        if id == category.get("id"):
            attributes = category.get("attributes")
            name = category.get("name")
            break

    '''adding the field to make it backend application compatible'''
    if not None in (attributes, name):
        for attribute in attributes:
            attribute["field"] = attribute.get("handle").replace("-", "_")
            attribute.pop("id")
            attribute.pop("extended")
    else:
        return ["error", "invalid vertical vertical for the id"]

    asyncio.create_task(mongodb.Write.category_schema(id, name, attributes))
    return attributes


@check_taxonomy
@alru_cache(maxsize=128, ttl=900)
async def top_level_attributes():
    taxonomy = current_app.taxonomy
    
    top_level = []
    for index, vertical in enumerate(taxonomy):  # categorising category properties

        top_level.append({
            "name": vertical["categories"][0].get("name"),
            "full_name": vertical["categories"][0].get("full_name"),
            "id": vertical["categories"][0].get("id"),
            "vertical": index
        })
    return top_level


@check_taxonomy
@alru_cache(maxsize=128, ttl=900)
async def next_level_attributes(id: str, vertical: int) -> list:
    next_level = []
    level = None
    taxonomy = current_app.taxonomy


    for category in taxonomy[vertical].get("categories"): # taxonomy is a list collection of categories        
        if id == category.get("id"):
            print(category.get("id"), id)
            level = category.get("level")
    
    
        if None != level and category.get("level") == level+1 and id == category.get("parent_id"):
            
            next_level.append({
                "name": category.get("name"),
                "full_name": category.get("full_name"),
                "id": category.get("id"),
                "level": level + 1
            })

    return next_level



@alru_cache(maxsize=128, ttl=900)
@check_taxonomy
async def get_product_attributes(category_id, vertical):
    """Get the object of product attributes including `listing_attributes`, `category_attributes`
    and `image_attributes`. 

    Parameters:
        - category_id (str) -> taxonomy category id
        - vertical (int) -> level0 vertical index number

    Returns:
        On success:
            - tuple[dict[str, list], int]
                index 0 -> dict contaning the attributes
                index 1 -> rest api status
        On failure:
            - tuple[dict[str, str], int]
                index 0 -> dict containing `"status"` and `"message"`
                index 1 -> rest api status
    """
    product_attributes = await asyncio.gather(mongodb.Fetch.listing_schema(), mongodb.Fetch.category_schema(category_id),
                   mongodb.Fetch.image_schema(category_id))
    
    # print(product_attributes)
    catalog_schema = product_attributes[0]
    category_schema = product_attributes[1]
    image_attributes = product_attributes[2]

    if catalog_schema.get('error') is not None:
        return {"status": "interrupted", "message": "attributes are not available for this product"}, 500

    category_attributes = category_schema.get("attributes")  if category_schema.get("attributes") != None else await get_taxonomy_attributes(vertical, category_id)

    if category_attributes[0] == "error":
        return {"status": "failed", "message": "invalid vertical for the category id"}, 400
    
    return {
        "listing_attributes": catalog_schema.get("attributes"),
        "category_attributes": category_attributes,
        "image_attributes": image_attributes.get("attributes")
    }, 200



# @alru_cache(maxsize=128, ttl=900)
@check_taxonomy
async def get_product_bulkupload_workbook(category_id, vertical):
    """Get product bulk upload workbook for the desired category.
    
    Parameters:
        - category_id (str) -> category id from the category taxonomy api
        - vertical (int) -> vertical index from the top level taxonomy api

    Returns:
        On success:
            - xlsx workbook (bytes)
            - tuple[dict[str, str], int]
                index 0 -> dict containing `status` and `message`
                index 1 -> rest api status
    """
    attributes = await asyncio.gather(mongodb.Fetch.listing_schema(), 
                                          mongodb.Fetch.category_schema(category_id))

    category_attribute = attributes[1].get("attributes") if attributes[1].get("attributes") is not None else get_taxonomy_attributes(vertical, category_id)
    '''IF THE ATTRIBUTE ISN'T IN THE DB AND WHEN SERCHEC THE VERTICAL THE VERTICAL IS INCORRECT'''
    if category_attribute[0] == "error":
        return {"status": "failed", "message": "invalid vertical for the category id"}, 400

    attributes = attributes[0].get("attributes") + category_attribute

    new_workbook = await asyncio.to_thread(workbook.create, attributes)
    return new_workbook