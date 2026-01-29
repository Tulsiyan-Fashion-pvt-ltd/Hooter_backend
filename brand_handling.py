from flask import Blueprint, session, request, jsonify
from database import mysql
from helper import User
import uuid
import datetime
import json

brand = Blueprint('brand', __name__)

# handling the database quiries related to brands to handle brands
class Write:
    @staticmethod
    def insert_brand(brand_id, brand_data):
        cursor = mysql.connection.cursor()
        try:
            query = """
                INSERT INTO brand (
                    brand_id,
                    entity_name,
                    brand_name,
                    niche,
                    gstin,
                    plan,
                    address,
                    est_year,
                    created_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cursor.execute(query, (
                brand_id,
                brand_data.get('entity-name'),
                brand_data.get('brand-name'),
                brand_data.get('niche'),
                brand_data.get('gstin'),
                brand_data.get('plan'),
                brand_data.get('address'),
                brand_data.get('estYear'),
                datetime.datetime.now()
            ))
            mysql.connection.commit()
        except Exception:
            mysql.connection.rollback()
            raise
        finally:
            cursor.close()

    @staticmethod
    def insert_poc(brand_id, poc_data):
        cursor = mysql.connection.cursor()
        try:
            query = """
                INSERT INTO poc (
                    poc_id,
                    brand_id,
                    name,
                    number,
                    email,
                    designation,
                    access
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            cursor.execute(query, (
                str(uuid.uuid4()),
                brand_id,
                poc_data.get('name'),
                poc_data.get('number'),
                poc_data.get('email'),
                poc_data.get('designation'),
                poc_data.get('access')
            ))
            mysql.connection.commit()
        except Exception:
            mysql.connection.rollback()
            raise
        finally:
            cursor.close()

    @staticmethod
    def map_user_brand(user_id, brand_id):
        cursor = mysql.connection.cursor()
        try:
            query = """
                INSERT INTO brand_access (brand_id, user_id)
                VALUES (%s,%s)
            """
            cursor.execute(query, (brand_id, user_id))
            mysql.connection.commit()
        except Exception:
            mysql.connection.rollback()
            raise
        finally:
            cursor.close()


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

    brand_data = response.get('brand')
    poc_data = response.get('poc')

    if not brand_data or not poc_data:
        return jsonify({'status': 'error', 'message': 'invalid payload'}), 400

    brand_id = Brand.create_id()

    try:
        Write.insert_brand(brand_id, brand_data)

        # Check if the user is self POC
        if poc_data.get('self') is True:
            # User is self POC - don't insert POC, just map user to brand
            user_id = session.get('user_id')
            if not user_id:
                return jsonify({'status': 'error', 'message': 'user not logged in'}), 401
            Write.map_user_brand(user_id, brand_id)
        else:
            # User is not POC - create new POC with generated user_id
            poc_user_id = User.create_userid()
            poc_data['user_id'] = poc_user_id
            Write.insert_poc(brand_id, poc_data)

        return jsonify({
            'status': 'ok',
            'brand_id': brand_id
        }), 201

    except Exception as e:
        print(e)
        return jsonify({'status': 'error', 'message': 'server error'}), 500


@brand.route('/request-niches', methods=['GET'])
async def request_niches():
    niches = Brand.fetch_niches()
    return jsonify({'status': 'ok', "niches": niches}), 200



if __name__ == "__main__":
    request_niches()