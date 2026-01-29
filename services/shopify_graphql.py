import requests


class ShopifyGraphQLClient:
    def __init__(self, shop_name: str, access_token: str, api_version: str = "2024-01"):
        self.endpoint = (
            f"https://{shop_name}.myshopify.com/"
            f"admin/api/{api_version}/graphql.json"
        )
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": access_token
        }

    def create_product(self, title: str, description: str, price: str) -> dict:
        query = """
        mutation productCreate($input: ProductInput!) {
          productCreate(input: $input) {
            product {
              id
              title
            }
            userErrors {
              field
              message
            }
          }
        }
        """

        variables = {
            "input": {
                "title": title,
                "descriptionHtml": description,
                "variants": [
                    {
                        "price": price
                    }
                ]
            }
        }

        response = requests.post(
            self.endpoint,
            json={
                "query": query,
                "variables": variables
            },
            headers=self.headers,
            timeout=15
        )

        response.raise_for_status()
        data = response.json()

        user_errors = data["data"]["productCreate"]["userErrors"]
        if user_errors:
            raise Exception(user_errors)

        return data["data"]["productCreate"]["product"]
