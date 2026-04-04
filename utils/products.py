from uuid import uuid4
from datetime import datetime
from sql_queries import catalogdb

async def create_usku():
    prefix = "Usku"
    unique_char = str(uuid4())[:9]
    unique_int = str(int(uuid4()))[:8]
    usku_count = await catalogdb.Fetch.count_catalogs()
    usku_count = str(usku_count)
    usku_id = prefix+unique_char+unique_int+str(datetime.now().date())+usku_count
    return usku_id

# print(create_usku())