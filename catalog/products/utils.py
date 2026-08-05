from uuid import uuid4
from datetime import datetime
import hashlib

def create_usku() -> str:
    """
    GENERATES USKU_ID
    """
    id = str(uuid4().hex)
    return id

def create_variant_id(usku_id: str) -> str:
    """
    GENERATES VARIANT ID
    """
    prefix = "variant_"
    id = usku_id + str(datetime.now())
    unique_hash = hashlib.sha256(id.encode("utf-8")).hexdigest()
    return prefix + unique_hash