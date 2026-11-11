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
            product_mariadb.Fetch.catalog_product(usku_id), 
            product_mongodb.Fetch.catalog_product(usku_id)
        )
    
    if product_listing_attributes.get('status') == 'failed':
        return {'status': 'failed', 'message': 'Error while fetching product listing attributes for variant'}, 500
    product_listing_attributes = product_listing_attributes.get('value')

    if product_category_attributes.get('error') == 'failed':
        return {'status': 'failed', 'message': 'Error while fetching product listing attributes for variant'}, 500

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
            return {'status': 'failed', 'message': 'Issue occured while Linking the uploaded products as variants', 'products_upload_status': upload_responses}, 200

        return {'status': 'successful', 'products_upload_status': upload_responses, 'variant_id': variants_link_response.get('value')}, 200
    else:
        return {'status': 'failed', 'message': 'Could not upload the variants', 'products_upload_status': upload_responses}, 500



@product_access_required
async def suggest_variant_sku_id(usku_id: str):
    """
    Suggest SKU ID  for the variant

    Args:
        usku_id: Univerally unique SKU ID

    Returns:
        Autogenrated sku id for suggestion
    """
    result = await asyncio.gather(mariadb.Fetch.count_variants(usku_id), 
                                         product_mariadb.Fetch.sku_id(usku_id))

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
async def get_product_variants(usku_id: str) -> list[dict]:
    """
    Get product variants information like usku_id and the listing and category attributes by which they vary

    Args:
        usku_id: USKU ID  of the product

    Returns:
        List of variants and their properties If exists or empty list including a main `webp_card` image for identity
    """
    listing_query = await mariadb.Fetch.variants(usku_id)

    if listing_query.get('status') != 'ok':
        return {'status': 'failed', 'message': 'Interal server error', 'code': '234c29aa'}, 500

    variants_listing_attributes = listing_query.get('value')
    variants_usku_ids = [variant.get('usku_id') for variant in variants_listing_attributes]

    await product_mongodb.Fetch.catalog_product(usku_ids=variants_usku_ids)
    return listing_query, 200
