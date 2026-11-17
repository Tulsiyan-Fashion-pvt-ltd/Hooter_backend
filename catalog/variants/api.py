from quart import Blueprint, request, session, jsonify
from . import models, services
from catalog.products.authorize import product_api_access_required
from utils.prerequirements import login_required, brand_required
from security_extensions import validate_csrf
from catalog.products import  mariadb as products_mariadb
from catalog.categories import mongodb as categories_mongodb
import asyncio
from traceback import print_exc
from pydantic import ValidationError


variants = Blueprint("variants", __name__, url_prefix = '/variants')


@variants.post('/<usku_id>')
@validate_csrf
@login_required
@brand_required
@product_api_access_required
async def create_product_variants(usku_id: str):
    """
    Create new product variants
    """
    payload = await request.get_json()

    type_id = await products_mariadb.Fetch.product_category_id(usku_id)
    if type_id.get('status') != 'ok':
        return jsonify({'status': 'failed', 'message': 'Error while fetching the category ID', 'error': type_id.get('error'), 'code': '8c5952ca'}), 500

    type_id = type_id.get('value')
    try:
        listing_attributes_schema, category_attributes_schema = await asyncio.gather(categories_mongodb.Fetch.listing_schema(),
                                                      categories_mongodb.Fetch.category_schema(type_id))

        if listing_attributes_schema.get('error') or category_attributes_schema.get('error'):
            return jsonify({'status': 'failed', 
                    'message': 'Error occured while fetching the product attributes', 
                    'code': '6e85ffee', 
                    'error': f'''listing_attribute_error = {listing_attributes_schema.get('error')}, 
                              category_attribute_error={category_attributes_schema.get('error')}'''
                    }), 500
        
        model = models.CreateVariantsModels(listing_attributes_schema.get('attributes'), category_attributes_schema.get('attributes'))
        variants = model.CreateVariantsModel.model_validate(payload).model_dump(exclude_unset=True)
    except ValidationError as e:
        print(e)
        return jsonify({'status': 'failed', 'message': 'Invalid payload', 'errors': e.errors(include_context=False), 'code': '43e11e60'}), 422    
    except Exception as e:
        print_exc()
        return jsonify({'status': 'failed', 'message': 'Unexpected error while validating payload', 'code': '5e7e6f27', 'error': str(e)}), 500

    try:
        service = await services.create_variants(usku_id, type_id, variants.get('variants'), session.get('brand'))
        return jsonify(service[0]), service[1]
    except Exception as e:
        print_exc()
        return jsonify({'status': 'failed', 'message': 'Could not upload the variants', 'error': str(e)}), 500



@variants.get('suggested-sku/<usku_id>')
@login_required
@brand_required
@product_api_access_required
async def get_variant_sku_id_suggestion(usku_id):
    suggestion = await services.suggest_variant_sku_id(usku_id)
    return jsonify(suggestion[0]), suggestion[1]



@variants.get("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def get_variants(usku_id: str):
    variants_service = await services.get_product_variants(usku_id)
    return jsonify(variants_service[0]), variants_service[1]