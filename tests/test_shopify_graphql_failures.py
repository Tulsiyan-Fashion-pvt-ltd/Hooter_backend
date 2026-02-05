import unittest
from unittest.mock import patch, Mock

from services.shopify_graphql import ShopifyGraphQLClient
from services.exceptions import ShopifyAPIError


class TestShopifyGraphQLFailures(unittest.TestCase):
    def setUp(self):
        self.client = ShopifyGraphQLClient("test-shop", "test-token")

    @patch("services.shopify_graphql.requests.post")
    def test_create_product_user_errors_raises(self, mock_post):
        mock_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "data": {
                    "productCreate": {
                        "product": None,
                        "userErrors": [{"field": ["title"], "message": "Title is required"}]
                    }
                },
                "extensions": {"cost": {"throttleStatus": {"currentlyAvailable": 1000, "maximumAvailable": 1000}}}
            },
            raise_for_status=lambda: None
        )

        with self.assertRaises(ShopifyAPIError):
            self.client.create_product_with_variants({"title": ""})

    @patch("services.shopify_graphql.requests.post")
    def test_create_product_media_user_errors_raises(self, mock_post):
        mock_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "data": {
                    "productCreateMedia": {
                        "media": None,
                        "userErrors": [{"field": ["media"], "message": "Invalid media"}]
                    }
                },
                "extensions": {"cost": {"throttleStatus": {"currentlyAvailable": 1000, "maximumAvailable": 1000}}}
            },
            raise_for_status=lambda: None
        )

        with self.assertRaises(ShopifyAPIError):
            self.client.create_product_media("gid://shopify/Product/1", "http://bad-url", "bad")

    @patch("services.shopify_graphql.requests.post")
    def test_product_update_user_errors_raises(self, mock_post):
        mock_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "data": {
                    "productUpdate": {
                        "product": None,
                        "userErrors": [{"field": ["title"], "message": "Invalid"}]
                    }
                },
                "extensions": {"cost": {"throttleStatus": {"currentlyAvailable": 1000, "maximumAvailable": 1000}}}
            },
            raise_for_status=lambda: None
        )

        with self.assertRaises(ShopifyAPIError):
            self.client.update_product("gid://shopify/Product/1", {"title": ""})


if __name__ == "__main__":
    unittest.main()