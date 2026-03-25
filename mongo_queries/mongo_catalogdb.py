from quart import current_app, jsonify

class Write:
    # function to add the catalog into the mongodb server
    async def single_catalog(catalog):
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
    async def catalog_attributes(niche_id):
      mongo = current_app.mongo

      try:
          # Ensure correct type (adjust if needed)
          niche_id = int(niche_id)

          doc = await mongo.db.product_info_schema.find_one({
              "niche_id": niche_id
          })

          if not doc:
              return {"error": "Not found"}
          
          # Convert ObjectId to string
          doc.pop("_id")

          return doc
      except Exception as e:
          print(e)
          return {"error": str(e)}
    

    