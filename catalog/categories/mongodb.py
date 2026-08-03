from quart import current_app

# returns the only niche specific keys without the type_id
def get_keys(doc):
    doc.pop("type_id") # taking out the type_id field from the attributes

    # LIST OF ALL THE KEYS FOR NICHE SPECIFIC ATTRIBUTES FOR MONGODB
    return doc.keys()



class Fetch:
    # fetch catalog attributes
    async def category_schema(type_id: str):
      """
      SCHEMA IMPORTANT FOR PRODUCT CATEGORY SPECIFIC
      """
      mongo = current_app.mongo

      try: 
            doc = await mongo.db.product_info_schema.find_one({"type_id": type_id}, {"_id": 0, "type_id": 0})  
            if not doc:
                return {"error": "Not found"}

            return doc
      except Exception as e:
            print(e)
            return {"error": str(e)}


    async def catalog_schema():
          """
          SCHEMA IMPORTANT FOR LISTING
          """
          mongo = current_app.mongo
    
          try:
                doc = await mongo.db.universal_catalog_schema.find_one({}, {"_id": 0, "type_id": 0})  
                if not doc:
                    return {"error": "Not found"}
    
                return doc
          except Exception as e:
                print(e)
                return {"error": str(e)}


    # fetching the image schema from the sql based upon the product type id
    async def image_schema(type_id: str):
        mongo = current_app.mongo
        try:
            doc = await mongo.db.image_schema.find_one({"type_id": type_id}, {"_id": 0, "type_id": 0})
            if not doc:
                return {"error": "Not found"}
            
            return doc
        except Exception as e:
            print(e)
            return {"error": str(e)}
        
          
    

    # fetch all the attributes of the product or stock
    class attributes():
        """
        LISTS OF ATTRIBUTES
        """

        class category():
            """
            Listing specific attributes
            """
            def __init__(self, type_id: str):
                self.type_id = type_id

        
            # fetch only mandatory schema keys of any niche
            async def mandatory(self):
                doc = await Fetch.catalog_shema(self.type_id)
                if not doc:
                  return None

                attributes = doc.get("attributes")
                category_mandatory_keys = [attribute.get("field") for attribute in attributes if attribute.get("required") == True]
                return category_mandatory_keys


            # to fetch all the attribute schema any niche id
            async def all(self):
                doc = await Fetch.category_schema(self.type_id)                               
                if not doc:
                  return None
               
                attributes = doc.get("attributes")
                category_keys = [attribute.get("field") for attribute in attributes ]
                return category_keys


        class catalog():
            """
            Catalog specific attribute
            """

            # fetch only mandatory schema keys of any niche
            async def mandatory():
                doc = await Fetch.catalog_shema()
                if not doc:
                  return None
                
                attributes = doc.get("attributes")
                universal_mandatory_keys = [attribute.get("field") for attribute in attributes if attribute.get("required") == True]
                return universal_mandatory_keys
            # to fetch all the attribute schema any niche id
            

            async def all(self):
                doc = await Fetch.catalog_shema()
                if not doc:
                  return None
                
                attributes = doc.get("attributes")
                universal_mandatory_keys = [attribute.get("field") for attribute in attributes]
                return universal_mandatory_keys