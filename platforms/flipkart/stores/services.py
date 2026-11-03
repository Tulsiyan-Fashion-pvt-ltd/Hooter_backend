import time
import redis_cache
from platforms.flipkart.auth.helper import get_access_token
from platforms.flipkart.stores.mariadb import Fetch, Write
from utils.encryption import TokenEncryption


class FlipkartService:
    @staticmethod
    async def get_valid_access_token(brand_id: str) -> str:
        """
        Return a valid Flipkart access token for a brand.
        Refresh the token when it is missing from Redis.
        """
        account = await Fetch.get_account(brand_id)

        if not account:
            raise ValueError("No Flipkart account is connected to this brand")

        cache_key = f"flipkart:{brand_id}:access_token"

        # Check Redis for the existing access token.
        cached_token = await redis_cache.get(cache_key)

        if cached_token:
            return TokenEncryption.decrypt_token(cached_token)

        # Fetch.get_account() should decrypt the refresh token.
        refresh_token = account["refresh_token"]

        if not refresh_token:
            raise ValueError("Flipkart refresh token is missing")

        # Request a new access token from Flipkart.
        token_data = await get_access_token(refresh_token)

        new_access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in")

        if not new_access_token or expires_in is None:
            raise RuntimeError("Flipkart returned an incomplete token response")

        try:
            expires_in = int(expires_in)
            if expires_in <= 0:
                raise ValueError
        except (TypeError, ValueError):
            raise RuntimeError("Flipkart returned an invalid access token expiry")

        # Update the refresh token in MariaDB if Flipkart returns a replacement.
        new_refresh_token = token_data.get("refresh_token")
        new_refresh_token_expires_in = token_data.get("refresh_token_expires_in")

        if new_refresh_token:
            result = await Write.update_tokens(
                brand_id=brand_id,
                refresh_token=new_refresh_token,
                refresh_token_expires_in=new_refresh_token_expires_in,
            )

            if result.get("status") != "ok":
                raise RuntimeError("Unable to save the refreshed Flipkart refresh token")

        # Encrypt the access token and cache it in Redis.
        encrypted_access_token = TokenEncryption.encrypt_token(new_access_token)

        await redis_cache.set(
            cache_key,
            encrypted_access_token,
            expires_in=expires_in,
        )

        return new_access_token