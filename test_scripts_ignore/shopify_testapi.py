import os
from dotenv import load_dotenv

load_dotenv()
import os
from services.shopify_graphql import ShopifyGraphQLClient

client = ShopifyGraphQLClient(
    shop_name=os.getenv("SHOPIFY_SHOP_NAME"),
    access_token=os.getenv("SHOPIFY_ADMIN_TOKEN")
)

product = client.create_product(
    title="Test Product",
    description="<p>Created via GraphQL</p>",
    price="199.00"
)

print(product)
