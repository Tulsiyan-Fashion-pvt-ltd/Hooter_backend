from quart import current_app
from asyncmy.cursors import DictCursor
from traceback import print_exc
from .authorize import colletion_db_brand_access


class Write:
    @staticmethod
    async def create_collection(name: str, brand_id: str) -> str| None:
        """
        create_collection creates the new collection for the brand and adds the enlisted products into it

        Args:
            name: name of the collection that brand would like to create
            brand_id: unique identifier for the brand

        Returns:
            On success:
                `collection ID` (str)
            On failure:
                `None`
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    '''CREATING NEW COLLECTION'''
                    collection_query = '''INSERT INTO collection_records (name, brand_id)
                    VALUES = (%s, %s)
                    '''
                    collection_value = (name, brand_id)

                    await cursor.execute(collection_query, collection_value)
                    collection_id = cursor.lastrowid
                    return collection_id
            except Exception as e:
                print(e)
                print_exc()
                return None



    @staticmethod
    @colletion_db_brand_access
    async def add_products_to_collection(collection_id: str, usku_ids: list[str]) -> str| None:
        """
        Add multiple products to a collection.

        Creates entries in the `product_collections` junction table by linking
        each USKU ID to the specified collection ID.

        Args:
            collection_id: The unique identifier of the collection.
            usku_ids: A list of product USKU IDs to be added to the collection.

        Returns:
            ok -> On success:
            None -> On failure
        """
        if not usku_ids:
            return
        
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = """
                        INSERT INTO product_collections (collection_id, usku_id)
                        VALUES (%s, %s)
                    """

                    values = [(collection_id, usku_id) for usku_id in usku_ids]

                    async with connection.cursor() as cursor:
                        await cursor.executemany(query, values)

                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(e)
                print_exc
                return None



    @staticmethod
    @colletion_db_brand_access
    async def remove_products_from_collection(
        collection_id: str,
        usku_ids: list[str]
    ) -> str | None:
        """
        Remove multiple products from a collection.

        Deletes the mappings between the given collection ID and each USKU ID
        from the `product_collections` junction table.

        Args:
            collection_id: The unique identifier of the collection.
            usku_ids: A list of product USKU IDs to remove.

        Returns:
            "ok" if the operation succeeds, otherwise None.
        """
        if not usku_ids:
            return None

        pool = current_app.pool

        async with pool.acquire() as connection:
            try:
                query = """
                    DELETE FROM product_collections
                    WHERE collection_id = %s AND usku_id = %s
                """

                values = [(collection_id, usku_id) for usku_id in usku_ids]

                async with connection.cursor(cursor=DictCursor) as cursor:
                    await cursor.executemany(query, values)

                await connection.commit()
                return "ok"

            except Exception as e:
                print(e)
                print_exc()
                return None



    @staticmethod
    @colletion_db_brand_access
    async def remove_collection(collection_id: str, brand_id: str) -> str | None:
        """
        Remove multiple products from a collection.
    
        Deletes entire collection from the `collection_records` table and junction table

        Args:
            collection_id: The unique identifier of the collection.
            brand_id: unique identifier for the brand

        Returns:
            "ok" if the operation succeeds, otherwise None.
        """
        pool = current_app.pool

        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = """
                        DELETE FROM collection_records
                        WHERE collection_id = %s
                        AND
                        brand_id = %s
                    """

                    values = (collection_id, brand_id)
                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(e)
                print_exc()
                return None




class Fetch:

    @staticmethod
    @colletion_db_brand_access
    async def fetch_collection_name(collection_id: str) -> str | None:
        """
        Fetch the name of a collection by its ID.

        Args:
            collection_id: The unique identifier of the collection.

        Returns:
            The collection name if found, otherwise `None`.
        """
        pool = current_app.pool

        async with pool.acquire() as connection:
            try:
                query = """
                    SELECT collection_name
                    FROM product_collections
                    WHERE collection_id = %s
                """

                async with connection.cursor(cursor=DictCursor) as cursor:
                    await cursor.execute(query, (collection_id,))
                    result = await cursor.fetchone()

                return result.get('collection_name') if result else None

            except Exception as e:
                print(e)
                print_exc()
                return None