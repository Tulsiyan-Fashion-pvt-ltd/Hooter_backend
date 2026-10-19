import time
from quart import current_app
from flipkart.auth.helper import get_access_token
from flipkart.stores.mariadb import Fetch, Write


class FlipkartService:
    @staticmethod
    async def get_valid_access_token(brand_id: str) -> str:
        """
        Return a valid Flipkart access token for a brand.
        Refresh the token when the access token has expired.
        """
        account = await Fetch.get_account(brand_id)

        if not account:
            raise ValueError("No Flipkart account is connected to this brand")

        access_token = account["access_token"]
        refresh_token = account["refresh_token"]
        access_token_expires_at = account["access_token_expires_at"]
        refresh_token_expires_at = account["refresh_token_expires_at"]

        # Return the existing access token if it has not expired.
        if access_token_expires_at > int(time.time()):
            return access_token

        # Check whether the refresh token has expired.
        if (
            refresh_token_expires_at is not None
            and refresh_token_expires_at <= int(time.time())
        ):
            raise ValueError(
                "Flipkart refresh token has expired. "
                "The seller must authorize the account again."
            )

        # Request a new access token from Flipkart.
        token_data = await get_access_token(refresh_token)
        new_access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in")

        if not new_access_token or expires_in is None:
            raise RuntimeError("Flipkart returned an incomplete token response")

        # Save the refreshed tokens in MariaDB.
        result = await Write.update_tokens(
            brand_id=brand_id,
            access_token=new_access_token,
            expires_in=expires_in,
            refresh_token=token_data.get("refresh_token"),
            refresh_token_expires_in=token_data.get("refresh_token_expires_in"),
        )

        if result.get("status") != "ok":
            raise RuntimeError("Unable to save refreshed Flipkart tokens")

        return new_access_token