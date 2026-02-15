import os
import sys
import unittest
from unittest.mock import patch, Mock

CURRENT_DIR = os.path.dirname(__file__)
BACKEND_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

from services.shopify_graphql import ShopifyGraphQLClient
from services.exceptions import ShopifyAPIError


class TestShopifyGraphQLFailures(unittest.TestCase):
    def setUp(self):
        self.client = ShopifyGraphQLClient("test-shop", "test-token")

    @patch("services.shopify_graphql.requests.post")
    def test_create_product_user_errors_raises(self, mock_post):
        # Mock REST API response for invalid product (empty title)
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.raise_for_status.side_effect = Exception("HTTP 422: Unprocessable Entity")
        mock_post.return_value = mock_response

        # create_product_with_variants uses REST API which raises HTTPError on 4xx/5xx
        with self.assertRaises(Exception) as context:
            self.client.create_product_with_variants({"title": ""})
        
        self.assertIn("422", str(context.exception))

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