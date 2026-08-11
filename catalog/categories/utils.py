from quart import current_app
from catalog.categories import mongodb
import asyncio


class Attributes:

    @staticmethod
    async def get(vertical: int, id: str):
        """
        EXTRACT CATEGORY ATTRIBUTE FROM THE SHOPIFY TAXONOMY AND RETURNS IT
        """

        '''It is used in the categories so if the we don't have the attribute data
            We can get it from the taxonomy
        '''
        taxonomy = current_app.taxonomy

        attributes = None
        for category in taxonomy[vertical].get("categories"):
            if id == category.get("id"):
                attributes = category.get("attributes")
                break

        #formatting the data that suites the mongodb schema
        if attributes:
            for attribute in attributes:
                attribute["field"] = attribute.get("handle").replace("-", "_")
                attribute.pop("id")
                attribute.pop("extended")

        asyncio.create_task(mongodb.Write.category_schema(id, attributes))
        return attributes