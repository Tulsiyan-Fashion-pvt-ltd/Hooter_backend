from quart import current_app
from typing import Literal




class Write:
    def __init__(self, ctx: dict | None = None):
        self.mongo = ctx["mongo"] if ctx is not None else current_app.mongo

    # function to add the catalog into the mongodb server
    async def product(self, catalog: dict):
        """Uploads category specific single product data in the mongodb

        Arguments:
            - catalog(dict): dict containg the attribute `field: str` and attribute `value: str| int| list| None`

        Returns:
            dict[str, str| None]:
                - error: None -> on success
                - error: str -> on failure

        """
        mongo = self.mongo
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
    async def bulk_product(self, products: list[dict[str, str| int| list| None]]) -> dict:
        mongo = self.mongo
        async with await mongo.cx.start_session() as connection:
          async with connection.start_transaction():
            try:
                await mongo.db.product_attributes.insert_many(products)
                return {"error": None}
            except Exception as e:
                connection.abort_transaction()
                print(e)
                return {"error": str(e)}
            
            
    async def update_catalog(self, usku_id, category_attributes: dict) -> str| dict:
        mongo = self.mongo
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


                
    async def delete_catalog(self, usku_id: str):
        mongo = self.mongo
        async with await mongo.cx.start_session() as connection:
            async with connection.start_transaction():
                try:
                    await mongo.db.product_attributes.delete_one({"usku_id": usku_id})
                    return "ok"
                except Exception as e:
                    connection.abort_transaction()
                    print(e)
                    return {"error": str(e)}



class Fetch:
    def __init__(self, ctx: dict | None = None):
        self.mongo = ctx["mongo"] if ctx is not None else current_app.mongo

    # fetch catalog product data
    async def catalog_product(self, usku_id: str|None = None, usku_ids: list|None=None) -> dict[Literal['status', 'value', 'error'], any]:
        """
        Returns the product category attributes in dict with `attributes` key when usku_id
            if usku_ids then list of `attributes` key
        """
        mongo = self.mongo
        try:
            doc = None

            if usku_id:
                doc = await mongo.db.product_attributes.find_one({"usku_id": usku_id}, {"_id": 0, "type_id": 0, "usku_id": 0})
            elif usku_ids:
                doc = mongo.db.product_attributes.find({"usku_id": {'$in': usku_ids}}, {"_id": 0, "type_id": 0})
                doc = await doc.to_list(length=None)
                
            if not doc:
                return {'status': 'ok', 'value': {}}

            return {'status': 'ok', 'value': doc}
        except Exception as e:
            print(e)
            return {"error": str(e), 'status': 'failed'}


    