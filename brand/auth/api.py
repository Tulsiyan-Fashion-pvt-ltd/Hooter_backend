from quart import Blueprint, session, request, jsonify, Response
from utils.helper import Payload
from brand.auth import mariadb
from utils.prerequirements import login_required, super_admin_required
import config
from traceback import print_exc
from .authorize import brand_api_access_required
from quart_rate_limiter import rate_limit, timedelta
from security_extensions import validate_csrf
from . import services



auth = Blueprint('auth', __name__)


@auth.route('/register', methods=['POST'])
@rate_limit(20, timedelta(minutes=60))
@validate_csrf
@login_required
# @super_admin_required
async def register_entity() -> Response:
    """Registers a new brand. If `poc.self` is `true`, 
    the authenticated user becomes the Point of Contact. Otherwise, 
    a new POC account is created as per the given poc details"""
    response = await request.get_json()    

    '''payload check'''
    required_payload = ['brand', 'poc']
    accepted_payload = required_payload
    valid_payload = (Payload.check_required_payload(response, required_payload) and 
                    Payload.check_accepted_payload(response, accepted_payload))
    if not valid_payload:
        return jsonify({'status': 'error', 'message': 'payload does not provide necessary values brand and poc'}), 400

    brand_data = response.get('brand')
    poc_data = response.get('poc')

    accepted_brand_payload = ['entity_name', 'brand_name', 'gstin', 'plan', 'address', 'city', 'state', 'pincode', 'estyear']
    required_brand_payload = ['entity_name', 'brand_name', 'plan', 'address', 'city', 'state', 'pincode', 'estyear']
    
    valid_payload = (Payload.check_required_payload(brand_data, required_brand_payload) and
                     Payload.check_accepted_payload(brand_data, accepted_brand_payload))

    if not valid_payload:
        return jsonify({'status': 'error', 'message': 'payload does not provide necessary values brand data'}), 400

    '''after checking payload for business trying to register the brand''' 
    user_id = session.get('user')
    # brand_data = {"brand_id": create_id(), **brand_data}

    try:
        '''register the brand'''
        brand_response = await services.register_brand(user_id, brand_data)
        brand_returned_code = brand_response.pop('code')
        brand_id = brand_response.get('brand_id')
        if 200 != brand_returned_code:
            return jsonify(brand_response), brand_returned_code

        '''set another poc account'''
        if poc_data.get('self') == False:
            #checking if all the requied field is there
            accepted_poc_payload = ['self', 'name', 'number', 'email', 'designation', 'access', 'password']
            required_poc_payload = accepted_poc_payload

            if not Payload.check_accepted_payload(poc_data, accepted_poc_payload):
                return jsonify({"status": "failed", "message": "Unrecognised poc payload"}), 400
            if not Payload.check_required_payload(poc_data, required_poc_payload):
                return jsonify({'status': 'failed', 'message': 'payload does not provide necessary values for poc'}), 400

            # fetch access allower access_specifiers
            access_specifier = config._access
            if poc_data['access'] not in access_specifier:
                return jsonify({'status': 'invalid input', 'message': 'access specifiers are not valid'}), 422

            '''Adding new poc'''
            poc_response = await services.add_new_poc(brand_id, poc_data)
            poc_response = response.get('code')
            return {'brand': brand_response, 'poc': poc_response}, 200 if 200 in (brand_returned_code, poc_response) else 201
            
        return jsonify({
            'status': 'ok',
            'message': 'brand registered successfully'
        }), 200

    except Exception as e:
        print(f'error encountered while registering the brand\n{e}')
        print_exc()
        return jsonify({'status': 'error', 'message': 'server error'}), 500



@auth.get('/connect')
@auth.get('/connect/<brand_id>')
@rate_limit(60, timedelta(minutes=1))
@validate_csrf
@login_required
@brand_api_access_required
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
    user_id = session.get('user')
    if brand_id == None:
        response = await services.connect_brand(user_id)
    else:
        response = await services.connect_brand_id(brand_id, user_id)

    code = response.pop('code')
    return jsonify(response), code