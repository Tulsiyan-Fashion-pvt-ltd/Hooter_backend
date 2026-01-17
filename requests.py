from flask import Blueprint, request, Response, jsonify
from database import *

request = Blueprint('request', __name__)

request.route('/signup', methods=['POST'])
def signup():
    data = request.get_json()
    number = data.get('number')
    email = data.get('email')
    password = data.get('password')
    designation = data.get('designation')

    if number and email and password and designation:
        # verify number and email
        signup_user()
    else:
        return jsonify({'status': 'Bad Request'}), 400
    
    return jsonify({'status': 'ok'}), 200