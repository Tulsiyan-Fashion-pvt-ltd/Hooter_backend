from platforms.shopify.graphql import ShopifyGraphQLClient

async def upload_product_to_shopify(shop_name: str, access_token: str, product_data: dict) -> dict:
    """Upload a product to Shopify using GraphQL."""
    client = ShopifyGraphQLClient(shop_name, access_token)
    mutation = """
      mutation {
        productCreate(product: 
        {
          title: "Cool socks", 
          category: "",
          
          productOptions: 
            [
              {name: "Color", values: [{name: "Red"}, {name: "Blue"}]}, 
              {name: "Size", values: [{name: "Small"}, {name: "Large"}]}
            ]
        }) 
        {
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
    response = await client.query(mutation)
    return response 