from quart import Blueprint, session, request, jsonify, Response
from utils.prerequirements import login_required, super_admin_required
from traceback import print_exc
from .authorize import brand_api_access_required
from quart_rate_limiter import rate_limit, timedelta
from security_extensions import validate_csrf
from . import services
from users.profile import mariadb as user_profile_maraidb
from .models import BrandRegisterModel



auth = Blueprint('auth', __name__)


@auth.route('/register', methods=['POST'])
@rate_limit(20, timedelta(minutes=60))
@validate_csrf
@login_required
@super_admin_required
async def register_entity() -> Response:
    """Registers a new brand. If `poc.self` is `true`, 
    the authenticated user becomes the Point of Contact. Otherwise, 
    a new POC account is created as per the given poc details"""
    payload = await request.get_json()    

    '''payload check'''
    try:
        payload = BrandRegisterModel.model_validate(payload)
        payload = payload.model_dump()

        poc_data = payload.get('poc')
        brand_data = payload.get('brand')
    except ValueError as e:
        print(e)
        return jsonify({'status': 'failed', 'message': 'Invalid payload', 'error': [{"loc": error.get('loc'),
                                                                                    "msg": error.get('msg')
                                                                                    }for error in e.errors()]}), 400
    except AttributeError as e:
        print(e)
        return jsonify({'status': 'failed', 'message': 'Error while validating payload', 'error': str(e)}), 500

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

        if poc_data.get('self') == True:
            user_data = await user_profile_maraidb.Fetch.user_details(session.get('user'))
            poc_data = {
                "name": user_data.get('user_name'),
                "number": user_data.get('phone_number'),
                "email": user_data.get('user_email'),
                "title": "Brand admin"
            }
            
        '''Adding new poc'''
        poc_response = await services.add_new_poc(brand_id, poc_data)
        poc_response_data = poc_response[0]
        poc_response_code = poc_response[1]
        return {'status': 'successful', 'brand': brand_response, 'poc': poc_response_data}, 200 if 200 in (brand_returned_code, poc_response_code) else 201
        
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
        response = await services.connect_brand_id(brand_id)

    code = response.pop('code')
    return jsonify(response), code