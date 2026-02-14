from flask import Blueprint, session, request, jsonify
from database import mysql, Write
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
def register_entity():
    user_id = session.get('user')
    if not user_id:
        return jsonify({'status': 'error', 'message': 'User not logged in'}), 401

    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'No data provided'}), 400

    brand_name = data.get('brand_name')
    if not brand_name:
        return jsonify({'status': 'error', 'message': 'brand_name is required'}), 400

    # Create the brand
    result = Write.create_brand(brand_name, user_id)
    if result.get('status') == 'ok':
        return jsonify(result), 201
    else:
        return jsonify(result), 400


@brand.route('/request-niches', methods=['GET'])
def request_niches():
    niches = Brand.fetch_niches()
    return jsonify({'status': 'ok', "niches": niches}), 200



if __name__ == "__main__":
    request_niches()