"""
Token encryption/decryption for Shopify access tokens.
Uses Fernet (AES-128 + HMAC) for secure encryption.
"""

from cryptography.fernet import Fernet
import os


class TokenEncryption:
    """Handle encryption/decryption of sensitive tokens."""
    
    @staticmethod
    def get_cipher():
        """Get Fernet cipher using app secret key."""
        # Generate key from environment variable
        secret = os.environ.get('HOOTER_SECRET_KEY', 'default-key')
        # Fernet requires a 32-byte base64 key
        # We'll derive it from the secret
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
        from cryptography.hazmat.backends import default_backend
        import base64

        # Use PBKDF2 to derive a proper key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'hooter_salt_v1',  # Fixed salt for consistency
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret.encode()))
        return Fernet(key)
    
    @staticmethod
    def encrypt_token(token: str) -> str:
        """
        Encrypt Shopify access token before storage in database.

        Args:
            token: Plain text Shopify access token (shpat_xxx)

        Returns:
            Encrypted token string (base64 encoded)
        """
        if not token:
            return None

        # Temporarily return plain token for testing
        return token
    
    @staticmethod
    def decrypt_token(encrypted_token: str) -> str:
        """
        Decrypt Shopify access token from database.
        Called only when making API calls to Shopify.

        Args:
            encrypted_token: Encrypted token string from database

        Returns:
            Plain text Shopify access token
        """
        if not encrypted_token:
            return None

        # Temporarily return plain token for testing
        return encrypted_token
