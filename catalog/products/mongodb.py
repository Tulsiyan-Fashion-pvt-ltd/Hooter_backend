from quart import current_app

# returns the only niche specific keys without the type_id
def get_keys(doc):
    doc.pop("type_id") # taking out the type_id field from the attributes

    # LIST OF ALL THE KEYS FOR NICHE SPECIFIC ATTRIBUTES FOR MONGODB
    return doc.keys()


class Write:
    # function to add the catalog into the mongodb server
    async def product(catalog: dict):
        """Uploads category specific single product data in the mongodb

        Arguments:
            - catalog(dict): dict containg the attribute `field: str` and attribute `value: str| int| list| None`

        Returns:
            dict[str, str| None]:
                - error: None -> on success
                - error: str -> on failure

        """
        mongo = current_app.mongo
        async with await mongo.cx.start_session() as connection:
          async with connection.start_transaction():
            try:
                await mongo.db.product_attributes.insert_one(catalog)
                return {"error": None}
            except Exception as e:
                connection.abort_transaction()
                print(e)
                return {"error": str(e)}
            

    # function to add the catalog into the mongodb server
    async def bulk_product(products: list[dict[str, str| int| list| None]]) -> dict:
        mongo = current_app.mongo
        async with await mongo.cx.start_session() as connection:
          async with connection.start_transaction():
            try:
                await mongo.db.product_attributes.insert_many(products)
                return {"error": None}
            except Exception as e:
                connection.abort_transaction()
                print(e)
                return {"error": str(e)}
            
            
    async def update_catalog(usku_id, category_attributes: dict) -> str| dict:
        mongo = current_app.mongo
        async with await mongo.cx.start_session() as connection:
            async with connection.start_transaction():
                try:
                    await mongo.db.product_attributes.update_one({"usku_id": usku_id},{
                            "$set": category_attributes
                        })
                    return "ok"
                except Exception as e:
                    connection.abort_transaction()
                    print(e)
                    return {"error": str(e)}


                
    async def delete_catalog(usku_id: str):
        mongo = current_app.mongo
        async with await mongo.cx.start_session() as connection:
            async with connection.start_transaction():
                try:
                    await mongo.db.product_attributes.delete_one({"usku_id": usku_id})
                    return "ok"
                except Exception as e:
                    connection.abort_transaction()
                    print(e)
                    return {"error": str(e)}



    async def variants(variants: list) -> dict:
        mongo = current_app.mongo
        try:
            await mongo.db.variants.insert_many(variants)
            return {"status": "ok"}
        except Exception as e:
            print(e)
            return {"error": str(e)}


class Fetch:
    # fetch catalog product data
    async def catalog_product(usku_id: str):
        mongo = current_app.mongo
        try:
            doc = await mongo.db.product_attributes.find_one({"usku_id": usku_id}, {"_id": 0, "type_id": 0, "usku_id": 0})

            if not doc:
                return {"error": "not found"}

            return doc
        except Exception as e:
            print(e)
            return {"error": str(e)}


    