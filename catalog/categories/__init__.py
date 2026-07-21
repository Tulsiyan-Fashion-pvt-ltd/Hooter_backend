from quart import Blueprint, current_app, jsonify, request
from utils.prerequirements import login_required, brand_required
from async_lru import alru_cache

categories = Blueprint("categories", __name__, url_prefix = "/categories")



@categories.get("/top")
@login_required
@brand_required
@alru_cache
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
@alru_cache
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