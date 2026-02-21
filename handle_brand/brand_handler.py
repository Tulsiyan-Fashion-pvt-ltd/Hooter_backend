from quart import Blueprint, session, request, jsonify
from handle_user.user_hanlderdb import Userdb
from helper import User, Helper
import uuid
import datetime
import json
from helper import User, Brand
from handle_brand.brand_handlerdb import Branddb

brand = Blueprint('brand', __name__)


# route to register the business
@brand.route('/register', methods=['POST'])
async def register_entity():
    response = await request.get_json()

    brand_data = response.get('brand')
    poc_data = response.get('poc')
    # print(brand_data, poc_data)


    # only allow the super_admin user to use this route
    user_access = Userdb.Fetch.user_access(session.get('user'))
    if (user_access == None or user_access != 'super_admin'):
        return jsonify({'status': 'access denied', 'message': 'you do not have the access kindly contact Hooter super admins'}), 401

    if not brand_data or not poc_data:
        return jsonify({'status': 'error', 'message': 'invalid payload'}), 400
    
    # except gstin any missing data will return 400 status
    for _ in brand_data:
        if _ != 'gstin' and (brand_data.get(_) is None or brand_data.get(_) == ''):
            return jsonify({'status': 'error', 'message': 'invalid payload'}), 400
    

    brand_id = Brand.create_id()
    user_id = session.get('user')
    #inserting the brand
    brand_data = {
        'entity_name': brand_data.get('entity-name') if brand_data.get('entity-name') and brand_data.get('entity-name') != '' else None,
        'brand_name': brand_data.get('brand-name') if brand_data.get('brand-name') and brand_data.get('brand-name') != '' else None,
        'niche': brand_data.get('niche') if brand_data.get('niche') and brand_data.get('niche') != '' else None,
        'gstin': brand_data.get('gstin') if brand_data.get('gstin') and brand_data.get('gstin') != '' else None,
        'plan': brand_data.get('plan') if brand_data.get('plan') and brand_data.get('plan') != '' else None,
        'address': brand_data.get('address') if brand_data.get('address') and brand_data.get('address') != '' else None,
        'estyear': brand_data.get('estyear') if brand_data.get('estyear') and brand_data.get('estyear') != '' else None
    }

    result =  Branddb.Write.insert_brand(brand_id, user_id, brand_data)
    if result == 'error':
        return jsonify({'status': 'failed', 'message': 'error occured while registering the brand'}), 500

    try:
        # Check if the user is self POC
        if poc_data.get('self') == 'true':
            # User is self POC - don't insert POC, just map user to brand
            user_id = session.get('user')
            if not user_id:
                return jsonify({'status': 'error', 'message': 'user not logged in'}), 401
            
            Branddb.Write.map_user_brand(user_id, brand_id)
        else:
            #checking if all the requied field is there
            required_field = ['self', 'name', 'number', 'email', 'designation', 'access', 'password']
            valid_payload = Helper.check_required_payload(poc_data, required_field)

            if valid_payload is not True:
                report = jsonify({'status': 'error', 'message': 'payload does not provide necessary values'}), 400
                print(report)
                return report
             
            # User is not POC - create new POC with generated user_id
            poc_user_id = User.create_userid()
            poc_data['user_id'] = poc_user_id

            # fetch access allower access_specifiers
            access_specifier = Brand.access_specifiers()
            
            # checking if user specified the access
            if poc_data['access'] not in access_specifier or poc_data.get('password')==None:
                return jsonify({'status': 'invalid input', 'message': 'access specifiers are not valid'}), 422

            user_creds={
                'userid': poc_user_id,
                'name': poc_data['name'] if poc_data['name'] and poc_data['name'] != '' else None,
                'number': poc_data['number'] if poc_data['number'] and poc_data['number'] != '' else None,
                'email': poc_data['email'] if poc_data['email'] and poc_data['email'] != '' else None,
                'access': poc_data['access'] if poc_data['access'] and poc_data['access'] != '' else None,
                'designation': poc_data['designation'] if poc_data['designation'] and poc_data['designation'] != '' else None,
                'hashed_password': User.hash_password(poc_data.get('password'))
                }
            Userdb.Write().signup_user(user_creds)
            Branddb.Write.map_user_brand(poc_user_id, brand_id)

        return jsonify({
            'status': 'ok',
            'message': 'registered the brand successfully'
        }), 201

    except Exception as e:
        print(f'error encountered while registering the brand\n{e}')
        return jsonify({'status': 'error', 'message': 'server error'}), 500


@brand.route('/request-niches', methods=['GET'])
def request_niches():
    niches = Brand.fetch_niches()
    return jsonify({'status': 'ok', "niches": niches}), 200



if __name__ == "__main__":
    request_niches()