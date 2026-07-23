from quart import current_app
from async_lru import alru_cache

@alru_cache(maxsize=32)
async def get(vertical: int, id: str):
    taxonomy = current_app.taxonomy

    attributes = None
    for category in taxonomy[vertical].get("categories"):
        if id == category.get("id"):
            attributes = category.get("attributes")
            break

    return attributes