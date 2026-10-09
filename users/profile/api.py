from quart import Blueprint, jsonify, session
from utils.prerequirements import login_required
from users.profile import mariadb
from security_extensions import validate_csrf
from utils.encryption import TokenEncryption

profile = Blueprint("profile", __name__, url_prefix="/profile")


@profile.get('')
@login_required
async def fetch_user_creds():
    user = session.get('user')
    
    if user==None:
        return jsonify({'status': 'unauthorised access', 'message': 'no loged in user found'}), 401
    _ = await mariadb.Fetch.user_details(user)

    user_data = {
                'name': TokenEncryption.decrypt_token(_.get('user_name')),
                'number': TokenEncryption.decrypt_token(_.get('phone_number')),
                'email': TokenEncryption.decrypt_token(_.get('user_email')),
                'designation': TokenEncryption.decrypt_token(_.get('user_designation'))
                }
    return jsonify({'status': 'ok', 'user_data': user_data}), 200