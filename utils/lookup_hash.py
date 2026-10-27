import hashlib
import hmac
import os
from dotenv import load_dotenv

load_dotenv()



def create_hash(text: str) -> str:
    normalized = text.strip().lower()

    return hmac.new(
        os.environ.get('HOOTER_SECRET_KEY').encode('utf-8'),
        normalized.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()