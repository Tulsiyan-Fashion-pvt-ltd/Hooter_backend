from datetime import datetime
import uuid
from argon2 import PasswordHasher

def create_userid() -> str:
    # create hooter user ids-
    prefix = 'user_'
    unique_id = str(uuid.uuid4())[:18]
    date = str(datetime.now().date()).replace('-', '')
    userid = prefix+unique_id+date
    return userid


def hash_password(password):
    return PasswordHasher().hash(password)

def verify_hashed_password(password: str, hashed_password: str) -> bool:
    try:
        PasswordHasher().verify(hashed_password, password)
        return True
    except Exception:
        return False
