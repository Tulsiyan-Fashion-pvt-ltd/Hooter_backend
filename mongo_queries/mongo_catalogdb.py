from quart import current_app, jsonify

class Write:
    pass

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
    

    async def product_niche_id(niche_name):
        mongo = current_app.mongo
        fields = mongo.db.niche.aggregate([
                   { "$unwind": "$sub_niches" },
                   { "$unwind": "$sub_niches.categories" },
                   { "$unwind": "$sub_niches.categories.products" },
                   {
                     "$match": {
                       "sub_niches.categories.products.name": "saree"
                     }
                   },
                   {
                     "$project": {
                       "_id": 0,
                       "product_id": "$sub_niches.categories.products.product_id"
                     }
                   }
                 ])
        
        return jsonify(fields)