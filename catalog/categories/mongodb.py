from quart import current_app
from traceback import print_exc
# returns the only niche specific keys without the type_id
def get_keys(doc):
    doc.pop("type_id") # taking out the type_id field from the attributes

    # LIST OF ALL THE KEYS FOR NICHE SPECIFIC ATTRIBUTES FOR MONGODB
    return doc.keys()


class Write:
    @staticmethod
    async def category_schema(id: str, name: str, attributes: dict) -> dict:
        """WRITES THE CATETGORY SCHEMA INTO THE MONGODB\n
        It used when the category schema is not available and need to search from taxonomy file.
        After reading from the file the data gets stored in the db.
        """
        mongo = current_app.mongo
        try:
            doc = await mongo.db.product_info_schema.insert_one({"type_id": id, "name": name, "attributes": attributes})
            return {"error": None, "status": "ok"}
        except Exception as e:
            print(f"error encountered while writing the category attributes\n{e}")
            return {"error": str(e), "status": "failed"}


class Fetch:
    async def category_name(type_id: str) -> dict[str, str]:
        '''Checks if a type_id exists in the category schema or not
        Arguments:
            type_id:
                category id from shopify taxonomy
        
        Returns:
            On success:
                dict[str, str]:
                    "error": "invalid type id"

            On failure:
                dict[str, str]:
                    "name": `str`
        '''
        try:
            mongo = current_app.mongo
            doc = await mongo.db.product_info_schema.find_one({"type_id": type_id}, {"_id": 1, "name": 1})
            return {"error": "invalid type id"} if not doc else doc
        except Exception as e:
            print(e)
            print_exc()
            return "error"


    async def category_schema(type_id: str) -> dict:
        """
        SCHEMA IMPORTANT FOR PRODUCT CATEGORY SPECIFIC\n
        Returns {"attributes": []}
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


    async def listing_schema():
        """
        SCHEMA IMPORTANT FOR LISTING\n
        Returns {"attributes": []}
        """
        mongo = current_app.mongo

        try:
              doc = await mongo.db.universal_catalog_schema.find_one({}, {"_id": 0})
              if not doc:
                  return {"error": "Not found"}

              return doc
        except Exception as e:
              print(e)
              return {"error": str(e)}


    # fetching the image schema from the sql based upon the product type id
    async def image_schema(type_id: str):
        """
        SCHEMA FOR IMAGE\n
        Returns {"attributes": []}
        """
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
    class Attributes():
        """
        LISTS OF ATTRIBUTES
        """

        class Category():
            """
            Listing specific attributes
            """
            def __init__(self, type_id: str):
                self.type_id = type_id

        
            # fetch only mandatory schema keys of any niche
            async def mandatory(self):
                doc = await Fetch.category_schema(self.type_id)
                if not doc:
                  return None

                attributes = doc.get("attributes")
                category_mandatory_keys = [attribute.get("field") for attribute in attributes if attribute.get("required") == True] if attributes else []
                return category_mandatory_keys


            # to fetch all the attribute schema any niche id
            #there is no point of checking all the fields since category fields an be many there is no way to cross check what data fields are getting stored
            # async def all(self):
            #     doc = await Fetch.category_schema(self.type_id)                               
            #     if not doc:
            #       return None
               
            #     attributes = doc.get("attributes")
            #     category_keys = [attribute.get("field") for attribute in attributes if attributes] if attributes else []
            #     return category_keys


        class Catalog():
            """
            Catalog specific attribute
            """
            async def mandatory():
                """fetch only mandatory attribute keys for catalog listing"""
                doc = await Fetch.listing_schema()
                if not doc:
                  return None
                
                attributes = doc.get("attributes")
                universal_mandatory_keys = [attribute.get("field") for attribute in attributes if attribute.get("required") == True]
                return universal_mandatory_keys
            

            async def all():
                """to fetch all the attribute schema for catalog listing"""
                doc = await Fetch.listing_schema()
                if not doc:
                  return None
                
                attributes = doc.get("attributes")
                universal_mandatory_keys = [attribute.get("field") for attribute in attributes]
                return universal_mandatory_keys


        class Images():
            """
            Image specific attribute
            """
            def __init__(self, type_id: str):
                self.type_id = type_id


            async def mandatory(self):
                """fetch only mandatory attribute keys for images"""
                doc = await Fetch.image_schema(self.type_id)

                if doc.get('error'):
                  return []
                
                attributes = doc.get("attributes")
                
                image_mandatory_keys = [attribute.get("field") for attribute in attributes if attribute.get("required") == True]
                return image_mandatory_keys

            
            # we don't need to check for all images. only mandatory images
            # async def all(self):
            #     """to fetch all the attribute schema for images"""
            #     doc = await Fetch.image_schema(self.type_id)
            #     if not doc:
            #       return None
                
            #     attributes = doc.get("attributes")
            #     all_image_keys = [attribute.get("field") for attribute in attributes]
            #     return all_image_keys