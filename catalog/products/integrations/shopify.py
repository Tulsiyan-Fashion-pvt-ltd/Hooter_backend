from platforms.shopify.graphql import ShopifyGraphQLClient

async def upload_product(store_id: int, product: dict):
    query = """
        mutation ProductCreate($product: ProductCreateInput!) {
        productCreate(product: $product) {
            product {
                id
            }
            userErrors {
                field
                message
            }
        }
    }
    """

    variables = {
            "product": {
                "title": product.get("product_title"),
                "category": product.get("type_id"),
                "descriptionHtml": product.get("product_description"),
                "productType": product.get("taxonomy_full_name"),
                "status": "ACTIVE",
                "tags": product.get("tags"),
                "vendor": product.get("vendor"),
            }
        }

    shopify_client = ShopifyGraphQLClient(store_id)
    response = await shopify_client.query(query, variables)

    if response.get("productCreate").get("userErrors") != []:
        return response.get("productCreate").get("userErrors")

    return response.get("productCreate").get("product").get("id")
    