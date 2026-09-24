from quart import Blueprint, request, Response, jsonify, session
from users.authentication import mariadb
from utils.helper import Validate
from utils.prerequirements import login_required
from brand.auth.services import connect_brand
from quart_rate_limiter import rate_limit
from datetime import timedelta
from security_extensions import validate_csrf
from utils.helper import Payload
from . import services
from brand.auth.services import connect_brand
from traceback import print_exc


auth = Blueprint("auth", __name__)


@auth.post('/signup')
@rate_limit(20, timedelta(minutes=60))
@validate_csrf
async def signup():
    data = await request.get_json()

    accepted_and_mandatory_payload = ['name', 'number', 'email', 'password', 'designation']
    if not (Payload.check_accepted_payload(data, accepted_and_mandatory_payload) and 
        Payload.check_required_payload(data, accepted_and_mandatory_payload)):
        return jsonify({'status': 'failed', 'message': 'All required field not provided'}), 400
    
    if not (Validate.email(data.get('email')) and Validate.in_phone_num(data.get('number'))):
        return jsonify({'status': 'failed', 'message': 'Invalid email or password'}), 422
    
    service_response = await services.signup_user(data)
    status_code = service_response.pop('code')
    return jsonify(status_code), status_code



@auth.post('/login')
@rate_limit(5, timedelta(minutes=15))
@validate_csrf
async def login():
    data = await request.get_json()
    if not data:
        return jsonify({'status': 'failed', 'message': 'JSON payload is not provided'}), 400

    email = data.get('email')
    password = data.get('password')

    #checking if the data is coming or not
    if not email or not password:
        return jsonify({'status': 'invalid request', 'message': 'Email or password not provided'}), 400

    if Validate.email(email):
        try:
            response = await services.login_user_with_email(email, password)
            login_code = response.pop('code')

            brand_access = await connect_brand(session.get('user'))
            brand_access.pop('code')
            return jsonify({'login': response, 'brand_connection': brand_access}), login_code
        except Exception as e:
            print(e)
            print_exc()
            return jsonify({'status': 'successful', 'message': 'Unexpected error occured while login and connecting the brand'}), 500
    else:
        return jsonify({'status': 'bad request', 'message': 'Invalid email'}), 401



@auth.get('/session')
async def check_session():
    '''Get user login status'''
    user = session.get('user')
    if user:
        return jsonify({'login': 'ok'}), 200
    else:
        return jsonify({'login': 'deny'}), 401
    

@auth.post('/logout')
@login_required
@validate_csrf
async def logout():
    session.clear()
    return jsonify({'status': 'ok', 'message': 'user logout'}), 200