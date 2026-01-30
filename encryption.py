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
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
        from cryptography.hazmat.backends import default_backend
        import base64
        
        # Use PBKDF2 to derive a proper key
        kdf = PBKDF2(
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
        
        try:
            cipher = TokenEncryption.get_cipher()
            encrypted = cipher.encrypt(token.encode())
            return encrypted.decode()
        except Exception as e:
            print(f"Error encrypting token: {str(e)}")
            raise Exception("Token encryption failed")
    
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
        
        try:
            cipher = TokenEncryption.get_cipher()
            decrypted = cipher.decrypt(encrypted_token.encode())
            return decrypted.decode()
        except Exception as e:
            print(f"Error decrypting token: {str(e)}")
            raise Exception("Token decryption failed - token may be corrupted")
