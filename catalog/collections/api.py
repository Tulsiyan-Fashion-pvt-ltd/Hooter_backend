from quart import Blueprint
from utils.prerequirements import login_required, brand_required
from .authorize import collection_api_brand_access_required

collections = Blueprint("collection", __name__, url_prefix = '/collections')


@collections.get("/test/<collection_id>")
@login_required
@brand_required
@collection_api_brand_access_required
async def collection_api_test(collection_id):
    print('running')
    return "pass"