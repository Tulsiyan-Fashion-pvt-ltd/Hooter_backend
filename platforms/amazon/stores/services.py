from . import mariadb
from utils.encryption import TokenEncryption

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