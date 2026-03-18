from quart import current_app, jsonify

class Write:
    pass

class Fetch:
    # fetch catalog attributes
    async def catalog_attributes(niche):
        mongo = current_app.mongo
        fields = mongo.db.niche.find({niche: niche})
        return jsonify(fields)
    
