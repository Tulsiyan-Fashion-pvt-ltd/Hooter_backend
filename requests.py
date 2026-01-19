from flask import Blueprint, request, Response, jsonify
from database import *
from helper import Validate, User, Helper

requests = Blueprint('request', __name__)

@requests.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    name=data.get('name')
    number = data.get('number')
    email = data.get('email')
    password = data.get('password')
    designation = data.get('designation')
    
    print(password)

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

        signup_user(user_creds)
        print('registered the user')
    else:
        return jsonify({'status': 'Bad Request'}), 400
    
    return jsonify({'status': 'ok'}), 200