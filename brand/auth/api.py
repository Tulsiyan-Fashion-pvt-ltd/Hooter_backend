from quart import Blueprint, session, request, jsonify, Response
from users.authentication import mariadb as usersql
from utils.helper import Payload, Brand
from users.helper import hash_password, create_userid
from brand.auth import mariadb
from utils.prerequirements import login_required, super_admin_required
import config
from traceback import print_exc

auth = Blueprint('auth', __name__)

# route to register the business
@auth.route('/register', methods=['POST'])
@login_required
# @super_admin_required
async def register_entity() -> Response:
    """Registers a new brand. If `poc.self` is `true`, 
    the authenticated user becomes the Point of Contact. Otherwise, 
    a new POC account is created as per the given poc details"""
    response = await request.get_json()    

    '''checking the payload for brand    '''
    required_payload = ['brand', 'poc']
    accepted_payload = required_payload
    valid_payload = (Payload.check_required_payload(response, required_payload) and 
                    Payload.check_accepted_payload(response, accepted_payload))

    if valid_payload is not True:
        return jsonify({'status': 'error', 'message': 'payload does not provide necessary values brand and poc'}), 400
    

    brand_data = response.get('brand')
    poc_data = response.get('poc')

    accepted_brand_payload = ['entity_name', 'brand_name', 'gstin', 'plan', 'address', 'city', 'state', 'pincode', 'estyear']
    required_brand_payload = ['entity_name', 'brand_name', 'plan', 'address', 'city', 'state', 'pincode', 'estyear']
    
    valid_payload = (Payload.check_required_payload(brand_data, required_brand_payload) and
                     Payload.check_accepted_payload(brand_data, accepted_brand_payload))

    if valid_payload is not True:
        return jsonify({'status': 'error', 'message': 'payload does not provide necessary values brand data'}), 400

    '''after checking payload for business trying to register the brand''' 
    user_id = session.get('user')
    brand_data = {"brand_id": Brand.create_id(), **brand_data}

    try:
        # Check if the user is self POC
        if bool(poc_data.get('self')) == True:
            result = await mariadb.Write.insert_brand(brand_data, user_id, access="brand_admin")
    
            if result == 1265:
                return jsonify({"status": "failed", "message": "Invalid value provided"})
            elif result == 1062:
                return jsonify({'status': 'failed', 'message': 'brand already exists'}), 409
            elif result != "ok":
                return jsonify({'status': 'failed', 'message': 'error occured while registering the brand'}), 500

        else:
            #checking if all the requied field is there
            accepted_poc_payload = ['self', 'name', 'number', 'email', 'designation', 'access', 'password']
            required_poc_payload = accepted_poc_payload

            if not Payload.check_accepted_payload(poc_data, accepted_poc_payload):
                return jsonify({"status": "failed", "message": "Unrecognised poc payload"}), 400

            if not Payload.check_required_payload(poc_data, required_poc_payload):
                report = jsonify({'status': 'failed', 'message': 'payload does not provide necessary values for poc'}), 400
                return report
             
            poc_user_id = create_userid()

            # fetch access allower access_specifiers
            access_specifier = config._access
            if poc_data['access'] not in access_specifier:
                return jsonify({'status': 'invalid input', 'message': 'access specifiers are not valid'}), 422

            user_creds={
                'userid': poc_user_id,
                'name': poc_data.get('name'),
                'number': poc_data.get('number'),
                'email': poc_data.get('email'),
                'access': poc_data.get('access'),
                'designation': poc_data.get('designation'),
                'hashed_password': hash_password(poc_data.get('password'))
            }
            
            insert_query = await mariadb.Write.insert_brand(brand_data, user_id, 'brand_admin')
            if insert_query != 'ok':
                if insert_query == 1062:
                    return jsonify({'status': 'failed', 'message': 'brand already exists'}), 409
                else: 
                    return jsonify({'status': 'error', 'message': 'error occured while registering the brand'}), 500
            
            poc_query = await usersql.Write.signup_user(user_creds)
            if poc_query != 'ok':
                if poc_query == 1062:
                    return jsonify({'status': 'failed', 'message': 'user already exists'}), 409
                else:
                    return jsonify({'status': 'error', 'message': 'error occured while adding the brand poc'}), 500

            access_query = await mariadb.Write.map_user_brand(brand_data.get('brand_id'), poc_user_id, 'brand_member')
            if access_query != 'ok':
                if access_query == 1062:
                    return jsonify({'status': 'failed', 'message': 'user already belongs to the brand'}), 409
                else:
                    return jsonify({'status': 'error', 'message': 'error occured while adding the brand poc'}), 500
            
        return jsonify({
            'status': 'ok',
            'message': 'brand registered successfully'
        }), 200

    except Exception as e:
        print(f'error encountered while registering the brand\n{e}')
        print_exc()
        return jsonify({'status': 'error', 'message': 'server error'}), 500



# request for brand access
@auth.get('/connect')
@auth.get('/connect/<brand_id>')
@login_required
async def connect_brand(brand_id=None) -> Response:
    '''
        Connects the brand with the user session.
        Check the brand access of the user from the database, whether there is any or many or none.
        If the brand_id is provided to connect, then the function will add the brand_id into the user session
        as `session["brand"]=brand_id` and if the brand id is not provided then it will return the list of brands 
        or None depending upon the brand registered for the logged in user.

        Parameters:
            - brand_id: str
            Brand id provided among the lists of brands provided by the same function

        Returns:
            Returns:
                dict: Serialized response containing the request status and,
                    when applicable, brand information.
    '''

    if brand_id is not None:
        if await mariadb.Fetch.check_brand_id(brand_id) == "available":
            session['brand'] = brand_id
            session.permanent = False
            return jsonify({"status": "successful", "message": "brand registered successfully"}), 200
        else:
            return jsonify({"status": "failed", "message": "invalid brand_id"}), 400

    user_id = session.get('user')
    brand_access = await mariadb.Fetch.brand_access(user_id)

    if brand_access is None:
        return jsonify({"status": "successful", "brands": None, "connection": "not connected", "message": "no brand is registered"}), 201
    elif len(brand_access) == 1:
        session['brand'] = brand_access[0].get('brand_id')
        print(f"{session.get('brand')} accessed by {session.get('user')}")
        return jsonify({"status": "successful", "brands": "single brand", "connection": "connected", "message": "brand connected successfully"}), 200
    else:
        return jsonify({"status": "successful", "bands": brand_access, "connection": "not connected", "message": "a brand needs to be selected"}), 201