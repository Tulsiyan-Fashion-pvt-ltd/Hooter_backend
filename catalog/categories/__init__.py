from quart import Blueprint, current_app, jsonify, request
from utils.prerequirements import login_required, brand_required
from async_lru import alru_cache
import asyncio
from . import mongodb

categories = Blueprint("categories", __name__, url_prefix = "/categories")



@categories.get("/top")
@login_required
@brand_required
@alru_cache(maxsize=32)
async def list_top_level_categories():
    taxonomy = current_app.taxonomy

    top_level = []
    for index, vertical in enumerate(taxonomy):  # categorising category properties

        top_level.append({
            "name": vertical["categories"][0].get("name"),
            "full_name": vertical["categories"][0].get("full_name"),
            "id": vertical["categories"][0].get("id"),
            "vertical": index
        })

    return jsonify({"level0": top_level})


@categories.get("/next")
@login_required
@brand_required
@alru_cache(maxsize=32)
async def list_next_level_categories():
    vertical = request.args.get("vertical", type=int)
    id = request.args.get("id")
    taxonomy = current_app.taxonomy

    """
        CHECKING THE REQUIREMENTS
    """
    if None in (vertical, id):
        return jsonify({"status": "request failed", "msg": "vertical(index) and category id is not provided"}), 400

        
    next_level = []
    level = None
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
    
    return jsonify({"next": next_level})


# some data are category specific soo for the front end to show them, it has to fetch it first
# this route will provide the data fields which for category specific attributes
@categories.get('/attributes')
@login_required
@brand_required
@alru_cache(maxsize=128)
async def get_attribute_fields():

    category_id = request.args.get('type', type=int)

    #sanitising the arguments
    if category_id is None:
        return jsonify({'status': "invalid argument", "msg": "no niche field available, it should be ?niche=<id>"}), 400
    else:
        try:
            category_id = int(category_id)
        except Exception as e:
            return jsonify({"staus": "invalid value", "msg": "the id should be int type"}), 422

    
    product_attributes = await asyncio.gather(mongodb.Fetch.catalog_schema(category_id), 
                   mongodb.Fetch.image_schema(category_id))
    
    # print(product_attributes)
    niche_attributes = product_attributes[0] 
    image_attributes = product_attributes[1] 

    if niche_attributes.get('error') is not None:
        return jsonify({"status": "interrupted", "msg": "attributes are not available for this product"}), 500
    
    return jsonify({
        "field_attributes": niche_attributes,
        "image_attributes": image_attributes
    })