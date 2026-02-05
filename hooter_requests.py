from flask import Blueprint, request, Response, jsonify, session
from database import Write, Fetch
from helper import Validate, User, Helper
from services.shopify_helpers import validate_shopify_token, ShopifyAPIError

requests = Blueprint('request', __name__)

@requests.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    name=data.get('name')
    number = data.get('number')
    email = data.get('email')
    password = data.get('password')
    designation = data.get('designation')
    
    if designation == None:
        designation = 'Owner'
    # verify number and email
    if number and email and password and designation and Validate.email(email) and Validate.in_phone_num(number):
        # verify number and email
        user_creds = {
            'name': name,
            'userid': User.create_userid(),
            'number': number,
            'email': email,
            'hashed_password': User.hash_password(password),
            'designation': designation
            }

        response = Write.signup_user(user_creds)

        if response and response.get('status') == 'error':
            if response.get('message') == 'user_already_registered':
                return jsonify({'status': 'already_registered'}), 409
        print('registered the user')
    else:
        return jsonify({'status': 'Bad Request', 'message': 'all required field not provided'}), 400
    
    return jsonify({'status': 'ok'}), 200



@requests.route('/login', methods=['POST'])
def login():
    #get the date from the api request
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    #checking if the data is coming or not
    if not email or not password:
        return jsonify({'status': 'invalid request', 'message': 'email or password not provided'}), 400

    if Validate.email(email):
        userid = Fetch.userid_by_email(email)

        # if the userid is null then return then do not log in
        if userid == None:
            return jsonify({'status': 'error', 'message': 'user not found with this email'}), 401
        hashed_password = User.hash_password(password)
        login_check = Fetch.check_password(userid, hashed_password)

        if login_check == 1:
            session['user'] = userid

            return jsonify({'status': 'ok', 'message': 'login successfull'}), 200
        else:
            return jsonify({'status': 'unauthorised', 'message': 'incorrect password'}), 401
    else:
        return jsonify({'status': 'bad request', 'message': 'invalid email'}), 400
    
# request to fetch user session
@requests.route('/session', methods=['GET'])
def check_session():
    user = session.get('user')

    if user:
        return jsonify({'login': 'ok'}), 200
    else:
        return jsonify({'login': 'deny'}), 401
    

@requests.route('/logout')
def logout():
    user = session.get('user')
    if not user:
        return jsonify({'status': 'error', 'message': 'not logged in'}), 401
    session.clear()
    return jsonify({'status': 'ok', 'message': 'user logout'}), 200


@requests.route('/request-user-credentials')
def fetch_user_creds():
    user = session.get('user')
    if user==None:
        return jsonify({'status': 'unauthorised access', 'message': 'no loged in user found'}), 401
    user_details = Fetch.user_details(user)
    if not user_details or len(user_details) < 5:
        return jsonify({'status': 'error', 'message': 'failed to fetch user details'}), 500
    user_data = {
                'name': user_details[0],
                'number': user_details[1],
                'email': user_details[2],
                'designation': user_details[3],
                'access': user_details[4]
                }
    return jsonify({'status': 'ok', 'user_data': user_data}), 200


@requests.route('/add-store', methods=['POST'])
def add_store():
    """
    Add a new Shopify store for the authenticated user.
    
    Required JSON fields:
    {
        "shopify_shop_name": "myshop",  # without .myshopify.com
        "shopify_access_token": "shpat_...",
        "store_name": "My Store Name" (optional),
        "is_primary": false  # optional, set true to make primary
    }
    """
    user_id = session.get('user')
    
    # Check authentication
    if not user_id:
        return jsonify({
            'status': 'unauthorised access',
            'message': 'no logged in user found'
        }), 401
    
    # Get request data
    data = request.get_json()
    
    if not data:
        return jsonify({
            'status': 'error',
            'message': 'Request body must be valid JSON'
        }), 400
    
    # Extract required fields
    shopify_shop_name = data.get('shopify_shop_name', '').strip()
    shopify_access_token = data.get('shopify_access_token', '').strip()
    store_name = data.get('store_name', '').strip() or shopify_shop_name
    is_primary = data.get('is_primary', False)
    
    # Validate required fields
    if not shopify_shop_name or not shopify_access_token:
        return jsonify({
            'status': 'error',
            'message': 'shopify_shop_name and shopify_access_token are required'
        }), 400
    
    # Validate Shopify token before storing
    try:
        validate_shopify_token(shopify_shop_name, shopify_access_token)
    except ShopifyAPIError as exc:
        return jsonify({
            'status': 'error',
            'message': str(exc)
        }), 400

    # Add store via database
    result = Write.add_store(
        user_id=user_id,
        shopify_shop_name=shopify_shop_name,
        shopify_access_token=shopify_access_token,
        store_name=store_name,
        is_primary=is_primary
    )
    
    # Return result from Write.add_store
    status_code = 201 if result.get('status') == 'ok' else 400
    return jsonify(result), status_code