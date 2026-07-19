from quart import current_app
import aiofiles
import asyncio
import json
from functools import lru_cache

@lru_cache(maxsize=128)
async def show_next_level(vertical: int, id: str):
    """
        SHOWS THE CATEGORY OF THE NEXT LEVEL FROM THE SELECTED CATEGORY
    """
    taxonomy = current_app.taxonomy

    next_level = []
    for category in taxonomy[vertical].get("categories"):
        level
        if id == category.get("id"):
            level = category.get("level")

        if category.get("level") == level+1 and id == category.get("parent_id"):
            next_level.append({
                "vertical": vertical,
                "name": category.get("name"),
                "full_name": category.get("full_name"),
                "id": category.get("id"),
                "level": level + 1
            })

    return next_level



# @current_app.before_serving
async def main():
    # await list_taxonomy()
    index = await show_next_level(0, "gid://shopify/TaxonomyCategory/ap")

    print([category.get("name") for category in index])



if __name__ == '__main__':
    asyncio.run(main())