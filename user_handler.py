from flask import Blueprint, request, Response, jsonify, session
from user_hanlderdb import Userdb
from helper import Validate, User, Helper
import asyncio

handle_user = Blueprint('handle_user', __name__)

@handle_user.route('/signup', methods=['POST'])
async def signup():
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

        response = await Userdb.Write.signup_user(user_creds)

        if response and response.get('status') == 'error':
            if response.get('message') == 'user_already_registered':
                return jsonify({'status': 'already_registered'}), 409
        print('registered the user')
    else:
        return jsonify({'status': 'Bad Request', 'message': 'all required field not provided'}), 400
    
    return jsonify({'status': 'ok'}), 200



@handle_user.route('/login', methods=['POST'])
async def login():
    #get the date from the api request
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    #checking if the data is coming or not
    if not email or not password:
        return jsonify({'status': 'invalid request', 'message': 'email or password not provided'}), 400

    if Validate.email(email):
        userid = Userdb.Fetch.userid_by_email(email)

        # if the userid is null then return then do not log in
        if userid == None:
            return jsonify({'status': 'error', 'message': 'user not found with this email'}), 401
        hashed_password = User.hash_password(password)
        login_check = Userdb.Fetch.check_password(userid, hashed_password)

        if login_check == 1:
            session['user'] = userid

            return jsonify({'status': 'ok', 'message': 'login successfull'}), 200
        else:
            return jsonify({'status': 'unauthorised', 'message': 'incorrect password'}), 401
    else:
        return jsonify({'status': 'bad request', 'message': 'invalid email'}), 400
    
# request to fetch user session
@handle_user.route('/session', methods=['GET'])
async def check_session():
    user = session.get('user')
    # print(user)
    if user:
        return jsonify({'login': 'ok'}), 200
    else:
        return jsonify({'login': 'deny'}), 401
    

@handle_user.route('/logout', methods=['POST'])
async def logout():
    print(session.get('user'))
    session.clear()
    print(session.get('user'))
    return jsonify({'status': 'ok', 'message': 'user logout'}), 200


@handle_user.route('/request-user-credentials')
async def fetch_user_creds():
    user = session.get('user')
    if user==None:
        return jsonify({'status': 'unauthorised access', 'message': 'no loged in user found'}), 401
    _ = Userdb.Fetch.user_details(user)
    user_data = {
                'name': _[0],
                'number': _[1],
                'email': _[2],
                'designation': _[3],
                'access': _[4]
                }
    return jsonify({'status': 'ok', 'user_data': user_data}), 200