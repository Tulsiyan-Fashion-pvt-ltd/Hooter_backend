from quart import current_app, jsonify

# returns the only niche specific keys without the type_id
def get_keys(doc):
    doc.pop("type_id") # taking out the type_id field from the attributes

    # LIST OF ALL THE KEYS FOR NICHE SPECIFIC ATTRIBUTES FOR MONGODB
    niche_specific_keys = doc.keys()
    return niche_specific_keys


class Write:
    # function to add the catalog into the mongodb server
    async def single_catalog(catalog: dict):
        mongo = current_app.mongo
        async with await mongo.cx.start_session() as connection:
          async with connection.start_transaction():
            try:
                await mongo.db.product_attributes.insert_one(catalog)
            except Exception as e:
                connection.abort_transaction()
                print(e)
                return {"error": str(e)}

class Fetch:
    # fetch catalog attributes
    async def catalog_schema(type_id: int):
      mongo = current_app.mongo

      try:
            # Ensure correct type (adjust if needed)
            type_id = int(type_id)  
            doc = await mongo.db.product_info_schema.find_one({
                        "type_id": type_id
                    })  
            if not doc:
                return {"error": "Not found"}

            # removing _id
            doc.pop("_id")
            doc.pop("type_id")  
            return doc
      except Exception as e:
            print(e)
            return {"error": str(e)}
      

    # fetching the image schema from the sql based upon the product type id
    async def image_schema(type_id: int):
        mongo = current_app.mongo
        try:
            doc = await mongo.db.image_schema.aggregate(
                        {"$match":{"type_id": type_id}}, 
                        {"$project":{"_id":0, "type_id": 0}}
                    ) 
            if not doc:
                return {"error": "Not found"}

            return doc
        except Exception as e:
            print(e)
            return {"error": str(e)}
          
    

    # fetch all the attributes of the product or stock
    class attributes():
        def __init__(self, type_id):
           self.type_id = type_id
        
        # fetch only mandatory schema keys of any niche
        async def mandatory(self):
            mongo = current_app.mongo
            doc = await mongo.db.universal_catalog_schema.find_one()

            if not doc:
              return None
            
            doc.pop("_id")

            universal_mandatory_keys = [ key for key in doc.keys() if doc.get(key) == "*" or "*" in doc.get(key)]

            niche_schema = await Fetch.catalog_schema(self.type_id)

            if niche_schema == None:
               return None
            
            niche_mandatory_keys = [key for key in niche_schema.keys() if niche_schema.get(key) == "*" or "*" in niche_schema.get(key)]

            universal_mandatory_keys.extend(niche_mandatory_keys) 
            return universal_mandatory_keys

        # to fetch all the attribute schema any niche id
        async def all(self):
            mongo = current_app.mongo
            doc = await mongo.db.universal_catalog_schema.find_one()

            if not doc:
              return None
            
            doc.pop("_id")

            universal_mandatory_keys = [ key for key in doc.keys()]

            niche_schema = await Fetch.catalog_schema(self.type_id)

            if niche_schema == None:
               return None
            
            niche_mandatory_keys = [key for key in niche_schema.keys()]

            universal_mandatory_keys.extend(niche_mandatory_keys) 
            return universal_mandatory_keys

        async def niche_specific(self):
            niche_schema = await Fetch.catalog_schema(self.type_id)

            if niche_schema == None:
               return None
            
            niche_mandatory_keys = [key for key in niche_schema.keys()]
 
            return niche_mandatory_keys