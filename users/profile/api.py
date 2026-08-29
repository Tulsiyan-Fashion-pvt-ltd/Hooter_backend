from quart import Blueprint, request, Response, jsonify, session
from utils.prerequirements import login_required
from users.profile import mariadb

profile = Blueprint("profile", __name__, url_prefix="/profile")

@profile.get('')
@login_required
async def fetch_user_creds():
    user = session.get('user')
    # print(user)
    if user==None:
        return jsonify({'status': 'unauthorised access', 'message': 'no loged in user found'}), 401
    _ = await mariadb.Fetch.user_details(user)

    user_data = {
                'name': _.get('user_name'),
                'number': _.get('phone_number'),
                'email': _.get('user_email'),
                'designation': _.get('user_designation')
                }
    return jsonify({'status': 'ok', 'user_data': user_data}), 200