from quart import Blueprint, request, Response, jsonify, session
from users.authentication import mariadb
from utils.helper import Validate, Helper
from users.helper import create_userid, hash_password, verify_hashed_password
from utils.prerequirements import login_required
from brand.auth.api import connect_brand


auth = Blueprint("auth", __name__)


@auth.post('/signup')
async def signup():
    data = await request.get_json()
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
            'userid': create_userid(),
            'number': number,
            'email': email,
            'hashed_password': hash_password(password),
            'designation': designation,
            }

        response = await mariadb.Write.signup_user(user_creds)

        if response != 'ok':
            if response == 1062:
                return jsonify({'status': 'failed', "mesasge": "user already registered"}), 409
            else:
                return jsonify({'status': 'failed', "mesasge": "could not signed up the user"}), 500
        print('registered the user')
    else:
        return jsonify({'status': 'Bad Request', 'message': 'all required field not provided'}), 400
    
    return jsonify({'status': 'ok'}), 200



@auth.post('/login')
async def login():
    data = await request.get_json()
    email = data.get('email')
    password = data.get('password')

    #checking if the data is coming or not
    if not email or not password:
        return jsonify({'status': 'invalid request', 'message': 'email or password not provided'}), 400

    if Validate.email(email):
        userid = await mariadb.Fetch.userid_by_email(email)

        # if the userid is null then return then do not log in
        if userid == None:
            return jsonify({'status': 'unauthorized', 'message': 'user not found with this email'}), 401

        hashed_password = await mariadb.Fetch.user_password(userid)
        login_check = verify_hashed_password(password, hashed_password.get("user_password"))
        
        if login_check == True:
            session.clear()
            session['user'] = userid
            session.permanent = False
            
            # a brand needs to link to the user
            # if no brand is linnked to the user then redirect to register
            brand_access = await connect_brand()
            return jsonify({"login": {'status': 'ok', 'message': 'login successful'}, "brand_connection": await brand_access[0].get_json(brand_access)}), 200
        else:
            return jsonify({'status': 'unauthorized', 'message': 'incorrect password'}), 401
    else:
        return jsonify({'status': 'bad request', 'message': 'invalid email'}), 401



# request to fetch user session
@auth.get('/session')
async def check_session():
    # print(request.cookies)
    user = session.get('user')
    # print(user)
    if user:
        return jsonify({'login': 'ok'}), 200
    else:
        return jsonify({'login': 'deny'}), 401
    

@auth.post('/logout')
@login_required
async def logout():
    session.clear()
    return jsonify({'status': 'ok', 'message': 'user logout'}), 200