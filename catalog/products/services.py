from catalog.products import mongodb 
from catalog.products import mariadb
from catalog.products.utils import create_variant_id, create_usku
from brand.auth import mariadb as brand_sql
import asyncio
from quart import session


class Variants:

    async def create(usku_id: str, variants: list) -> dict| str:
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





class Products:
    async def create(listing_attributes: dict, category_attributes: dict, category_id: str, taxonomy_full_name: str) -> dict| str:
        """
        RETURNS:\n
        If the operation is successful then {usku_id}\n
        else\n
        {"error": "error message"}
        """
        
        usku_id = create_usku()
        brand_name = await brand_sql.Fetch.brand_name_by_id(session.get("brand"))
    
        ## ADDING THE THE DATA IN THE SQL
        sql_attributes_value = {
        "brand_id": session.get("brand"),
        "usku_id": usku_id,                     
        "type_id": category_id,
        "taxonomy_full_name": taxonomy_full_name,
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
            return {"error": "Product has listed but could not store the product data\nTry updating the product details", "code": 202}

        return usku_id