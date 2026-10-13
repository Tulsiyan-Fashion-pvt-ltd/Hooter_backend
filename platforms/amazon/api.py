import os
import json
from urllib.parse import urlencode
from dotenv import load_dotenv
import aiohttp
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from . import mariadb
from .helper import get_access_token
from utils.encryption import TokenEncryption

load_dotenv()

class AmazonAPIClient:
    """AMAZON CLIENT FOR SP-API REQUESTS"""

    def __init__(self, brand_id: str, selling_partner_id: str):
        self.brand_id = brand_id
        self.selling_partner_id = selling_partner_id
        self.refresh_token = None
        self.access_token = None

        self.endpoint = os.getenv("AMAZON_SP_API_ENDPOINT")
        self.aws_region = os.getenv("AMAZON_AWS_REGION")

        self.headers = {
            "Content-Type": "application/json","User-Agent": os.getenv("AMAZON_USER_AGENT")
        }

    async def request( self, method: str, path: str, params: dict = None, body: dict = None) -> dict:
        # Get seller credentials from database
        await self.store_credentials()

        # Get temporary LWA access token
        self.access_token = await get_access_token(self.refresh_token)

        # Add LWA access token
        self.headers["x-amz-access-token"] = (self.access_token)

        # Convert request body to exact JSON bytes
        body_bytes = None
        if body is not None:
            body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")

        # Build URL including query parameters
        url = f"{self.endpoint}{path}"
        if params:
            url = f"{url}?{urlencode(params, doseq=True)}"

        # Sign the exact request
        signed_request = self.sign_request(method=method, url=url, headers=self.headers, body=body_bytes)

        # Send the exact signed request
        async with aiohttp.ClientSession() as session:
            async with session.request(method=method, url=url, data=body_bytes, headers=dict(signed_request.headers)) as response:
                response_text = await response.text()
                if response.status >= 400:
                    raise Exception(
                        f"Amazon SP-API request failed "
                        f"({response.status}): {response_text}"
                    )

                if not response_text:
                    return {}
                return json.loads(response_text)

    def sign_request(self, method: str, url: str, headers: dict, body: bytes = None):
        access_key = os.getenv("AMAZON_AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AMAZON_AWS_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            raise Exception("Amazon AWS credentials are not configured")

        if not self.aws_region:
            raise Exception("AMAZON_AWS_REGION is not configured")

        credentials = Credentials(access_key, secret_key)
        aws_request = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(credentials, "execute-api", self.aws_region).add_auth(aws_request)

        return aws_request

    async def store_credentials(self):
        store = await mariadb.Fetch.get_store(self.brand_id, self.selling_partner_id)
        if not store:
            raise Exception("Amazon store not found")

        encrypted_refresh_token = store.get(
            "amazon_refresh_token_encrypted"
        )

        if not encrypted_refresh_token:
            raise Exception("Amazon refresh token not found")

        self.refresh_token = (TokenEncryption.decrypt_token(encrypted_refresh_token))