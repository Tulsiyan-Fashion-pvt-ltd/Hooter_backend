from quart import session
from . import mariadb
from .utils import create_userid, hash_password, verify_hashed_password
from utils.helper import Validate




async def signup_user(user_data: dict) -> dict[str, str|int]:
    '''Sign up new user to the system
    
    Args:
    
    Returns:
        returns dict with status, message and code
        - on success code: 200
        - on failure, 409, 500
    '''
    user_creds = {
        'name': user_data.get('name'),
        'userid': create_userid(),
        'number': user_data.get('number'),
        'email': user_data.get('email'),
        'hashed_password': hash_password(user_data.get('password')),
        'designation': user_data.get('designation').capitalize() if user_data.get('designation') else 'Director',
    }

    response = await mariadb.Write.signup_user(user_creds)
    if response != 'ok':
        if response == 1062:
            return {'status': 'failed', "mesasge": "User already registered", 'code': 409} 
        else:
            return {'status': 'failed', "mesasge": "Could not signed up the user", 'code': 500} 
    else:
        return {'status': 'successful', 'message': 'Added the new user', 
                'user_id': user_creds.get('userid'), 'code': 200}



async def login_user_with_email(email: str, password: str) -> dict[str, dict| str| int]:
    '''Log the user with emila and password
    
    Args: 
        - email - Email of the user
        - password - given password for the user
        
    Returns:
        dict with rest api status code
        - on success - code -> 200
        - on failure - code -> 401
    '''
    if not Validate.email(email):
        return {'status': 'bad request', 'message': 'Invalid email', 'code': 401}
    
    userid = await mariadb.Fetch.userid_by_email(email)
    if userid == None:
        return {'status': 'unauthorized', 'message': 'User not found with this email', 'code': 401}

    hashed_password = await mariadb.Fetch.user_password(userid)
    login_check = verify_hashed_password(password, hashed_password.get("user_password"))

    if login_check == True:
        session.clear()
        session['user'] = userid
        session.permanent = False
        return {'status': 'successful', 'message': 'Login successful', 'code': 200}

    else:
        return {'status': 'unauthorized', 'message': 'Incorrect password', 'code': 401}