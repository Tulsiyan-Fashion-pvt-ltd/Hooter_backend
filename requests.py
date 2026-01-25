from flask import Blueprint, request, Response, jsonify, session
from database import Write, Fetch
from helper import Validate, User, Helper

requests = Blueprint('request', __name__)

@requests.route('/signup', methods=['POST'])
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
        userid = Fetch.check_email(email)

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