from uuid import uuid7


def create_usku() -> str:
    """
    GENERATES USKU_ID
    """
    uuid = str(int(uuid7()))
    suffix = 'usku-'
    id= suffix+uuid
    return id
