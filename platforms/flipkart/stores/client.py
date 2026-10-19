import json
import aiohttp
from platforms.flipkart.stores.services import FlipkartService


class FlipkartAPIClient:
    """Client for Flipkart Seller API requests."""

    def __init__(self, brand_id: str):
        self.brand_id = brand_id
        self.access_token = None
        self.endpoint = "https://api.flipkart.net"

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def request(self, method: str, path: str, params: dict = None, body: dict = None,) -> dict:
        """
        Send an authenticated request to the Flipkart Seller API.
        """

        # Get a valid access token for this brand.
        self.access_token = (
            await FlipkartService.get_valid_access_token(self.brand_i)
        )

        # Add the access token to the request headers.
        headers = {
            **self.headers,
            "Authorization": f"Bearer {self.access_token}",
        }

        # Convert the request body to JSON bytes.
        body_bytes = None
        if body is not None:
            body_bytes = json.dumps(
                body,
                separators=(",", ":"),
            ).encode("utf-8")

        # Build the request URL.
        url = f"{self.endpoint.rstrip('/')}/{path.lstrip('/')}"

        async with aiohttp.ClientSession() as http_session:
            async with http_session.request(
                method=method.upper(),
                url=url,
                params=params,
                data=body_bytes,
                headers=headers,
            ) as response:
                response_text = await response.text()

                if response.status >= 400:
                    raise RuntimeError(
                        f"Flipkart Seller API request failed "
                        f"({response.status}): {response_text}"
                    )

                if not response_text:
                    return {}

                try:
                    return json.loads(response_text)
                except json.JSONDecodeError:
                    raise RuntimeError("Flipkart returned an invalid JSON response")