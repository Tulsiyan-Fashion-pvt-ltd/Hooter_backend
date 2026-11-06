from uuid import uuid7


def create_usku() -> str:
    """
    GENERATES USKU_ID
    """
    uuid = str(int(uuid7()))
    suffix = 'usku-'
    id= suffix+uuid
    return id



def create_variant_id() -> str:
    """
    GENERATES VARIANT ID
    """
    prefix = "variant-"
    uuid = str(uuid7())
    id = prefix+uuid
    return id