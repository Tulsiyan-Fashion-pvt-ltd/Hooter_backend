from catalog.products.authorize import product_access_required
from catalog.products import mariadb as product_mariadb, mongodb as product_mongodb, services as product_services
import asyncio
from . import mariadb


@product_access_required
async def create_variants(usku_id: str, type_id, variants: list[dict[str, any]], brand_id: str) -> tuple[dict, int]:
    """
    Function to create the variants for the given listing or category attributes
    """
    product_listing_attributes, product_category_attributes = await asyncio.gather(
            product_mariadb.Fetch().catalog_product(usku_id),
            product_mongodb.Fetch().catalog_product(usku_id)
        )
    
    if product_listing_attributes.get('status') == 'failed':
        return {'status': 'failed', 'message': 'Error while fetching product listing attributes for variant'}, 500
    product_listing_attributes = product_listing_attributes.get('value')

    if product_category_attributes.get('error') == 'failed':
        return {'status': 'failed', 'message': 'Error while fetching product listing attributes for variant'}, 500
    product_category_attributes = product_category_attributes.get('value')

    upload_responses = []
    variants_usku_id = []
    for variant in variants:
        print("variant", variant)
        variant_listing_attributes = variant.get('listing_attributes', {})
        variant_category_attributes = variant.get('category_attributes', {})

        variant_sql_payload = {**product_listing_attributes, **variant_listing_attributes}
        variant_mongo_payload = {**product_category_attributes, **variant_category_attributes}

        product_create_response = await product_services.create_product(brand_id, variant_sql_payload, variant_mongo_payload, type_id)
        upload_responses.append(product_create_response[0])

        if product_create_response[0].get('usku_id'):
            variants_usku_id.append(product_create_response[0].get('usku_id'))


    if len(upload_responses) > 0:
        variants_link_response = await mariadb.Write.variants(usku_id, variants_usku_id)
        if variants_link_response.get('status') == 'failed':
            return {'status': 'failed', 'message': 'Issue occured while Linking the uploaded products as variants', 'products_upload_status': upload_responses}, 202

        return {'status': 'successful', 'products_upload_status': upload_responses, 'variant_id': variants_link_response.get('value')}, 200
    else:
        return {'status': 'failed', 'message': 'Could not upload the variants', 'error': "Unexpected error"}, 500



@product_access_required
async def suggest_variant_sku_id(usku_id: str) -> tuple[dict, int]:
    """
    Suggest SKU ID  for the variant

    Args:
        usku_id: Univerally unique SKU ID

    Returns:
        Autogenrated sku id for suggestion
    """
    result = await asyncio.gather(mariadb.Fetch.count_variants(usku_id), 
                                         product_mariadb.Fetch().sku_id(usku_id))

    variant_count = result[0]
    sku_id = result[1]

    if sku_id.get('value') is None:
        return  {
                    'status': 'failed', 
                    'message': 'Unable to fetch the product', 
                    'code': 'd6ec487d', 
                    'error': sku_id.get('error')
                }, 500

    if variant_count.get('value') == None:
        return  {
                    "sku_id": f"{sku_id.get('value')}-{1}"
                }, 500
    else:
        return {
            "sku_id": f"{sku_id.get('value')}-{int(variant_count.get('value'))+1}"
        }, 200




@product_access_required
async def get_product_variants(usku_id: str) -> tuple[dict, int]:
    """
    Get product variations on the classifications of product category attributes rather than lists of product variants
    """
    def get_variantions(category_variants: list) -> dict[str, dict]:
        # loop over category data and add unique attributes to their corresponding variant data
        variant_variations = {}  # store data as {usku_id: {key: value}}
        
        for category_variant in category_variants:
            variant_usku_id = category_variant.pop('usku_id')
            variant_variations[variant_usku_id] = {}

            for key in category_variant.keys():
                is_common = []  # common key in every variants
                
                for check_variant in category_variants:
                    if check_variant == category_variant:
                        continue

                    if check_variant.get(key) == category_variant.get(key):
                        is_common.append(True)
                    else:
                        is_common.append(False)

                if not all(is_common):
                    variant_variations[variant_usku_id][key] = category_variant.get(key)
        return variant_variations



    # this is a pending work
    def get_variant_dimentions(category_variants: list) -> dict[str, list[dict]]:
        variant_variations = get_variantions(category_variants)
        variant_dimention = {}  # {type: [{value and usku_id}]}
        for usku_id in variant_variations.keys():
            # we iterate over unique keys and club them out with values
            for key in variant_variations.get(usku_id).keys():

                variant_dimention[key] = []
                for inner_usku_id in variant_variations.keys():
                    variants = variant_variations.get(inner_usku_id)
                    variant_dimention[key].append({"usku_id": inner_usku_id, "value": variants.get(key)})

        return variant_dimention


    variants_list = await mariadb.Fetch.variants(usku_id)
    if variants_list.get('status') != 'ok':
        return {'status': 'failed', 'message': 'Interal server error', 'code': '1e9566fa'}, 500

    variants_list = variants_list.get('value')
    if len(variants_list) < 2:
        return [], 200  # variants supposed to be 2 at least

    usku_ids = [variant.get('usku_id') for variant in variants_list]

    variants_category_attr = await product_mongodb.Fetch().catalog_product(usku_ids = usku_ids)
    if variants_category_attr.get('status') == 'failed':
        return {'status': 'failed', 'message': 'Interal server error', 'code': '40cfdc99'}, 500

    variants_category_attr = variants_category_attr.get('value')

    products_category_variation = get_variantions(variants_category_attr)
    for variant in variants_list:
        variant["variations"] = products_category_variation.get(variant.get('usku_id'))



    return variants_list, 200
        

