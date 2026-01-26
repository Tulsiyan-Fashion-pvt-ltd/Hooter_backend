from flask import Blueprint, session, request, jsonify
from database import mysql
import uuid
import datetime
import json

brand = Blueprint('brand', __name__)

# handling the database quiries related to brands to handle brands
class Write:
    pass

class Fetch:
    pass


class Brand:
    @staticmethod
    def create_id() -> str:
        prefix = 'brand_'

        unique_id = str(uuid.uuid4())[:14]
        date = str(datetime.datetime.now().date()).replace('-', '')
        id = prefix+unique_id+date
        return id
    
    @staticmethod
    def fetch_niches() -> list:
        try:
            with open('./niche.json', 'r')as file:
                read = json.load(file)
            return (list(read.get('niche')[0].keys()))
        except Exception as e:
            print(f"error while reading the niche.json file as \n{e}")
            return list()


# route to register the business
@brand.route('/register', methods=['POST'])
async def register_entity():
    response = request.get_json()
    entity_name = response.get('entity-name')
    brand_name = response.get('brand-name')
    niche = response.get('niche')
    gstin = response.get('gstin')
    plan = response.get('plan')
    address = response.get('address')
    est_yr = response.get('est-yr')

    print(entity_name, brand_name, niche, gstin, plan, address, est_yr)
    return jsonify({'status': 'ok'}), 200

@brand.route('/request-niches', methods=['GET'])
async def request_niches():
    niches = Brand.fetch_niches()
    return jsonify({'status': 'ok', "niches": niches}), 200



if __name__ == "__main__":
    request_niches()