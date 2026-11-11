from uuid import uuid7

def create_variant_id() -> str:
    """
    GENERATES VARIANT ID
    """
    prefix = "variant-"
    uuid = str(uuid7())
    id = prefix+uuid
    return id