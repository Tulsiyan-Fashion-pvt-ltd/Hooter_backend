from quart import Blueprint, session, request, jsonify
from sql_queries import userdb
from utils.helper import User, Helper, Brand
from sql_queries import branddb
from utils.prerequirements import login_required, brand_required, super_admin_required
from utils import products

brand = Blueprint('brand', __name__)

# route to register the business
@brand.route('/register-brand', methods=['POST'])
@login_required
@super_admin_required
async def register_entity():
    response = await request.get_json()
    
    '''
        checking the payload for brand
    '''
    required_payload = ['brand', 'poc']
    accepted_payload = required_payload
    valid_payload = Helper.check_required_payload(response, accepted_payload, required_payload)

    if valid_payload is not True:
        return jsonify({'status': 'error', 'message': 'payload does not provide necessary values brand and poc'}), 400
    

    brand_data = response.get('brand')
    poc_data = response.get('poc')

    accepted_brand_payload = ['entity-name', 'brand-name', 'gstin', 'plan', 'address', 'pincode', 'estyear']
    required_brand_payload = ['entity-name', 'brand-name', 'plan', 'address', 'pincode', 'estyear']
    
    valid_payload = Helper.check_required_payload(brand_data, accepted_brand_payload, required_brand_payload)

    if valid_payload is not True:
        return jsonify({'status': 'error', 'message': 'payload does not provide necessary values brand-data and poc-data'}), 400

    '''
        after checking payload for business trying to register the brand
    '''

    brand_id = Brand.create_id()
    user_id = session.get('user')

    #inserting the brand
    brand_data = {
            'entity_name': brand_data.get('entity-name'),
            'brand_name': brand_data.get('brand-name'),
            'gstin': brand_data.get('gstin'),
            'plan': brand_data.get('plan'),
            'address': f"({brand_data.get('address')}, {brand_data.get('pincode')})",
            'estyear': brand_data.get('estyear')
        }        

    try:
        # Check if the user is self POC
        if poc_data.get('self') == 'true':

            # User is self POC - don't insert POC, just map user to brand
            result = await branddb.Write.insert_brand(brand_id, user_id, brand_data)

            if result == 'failed':
                return jsonify({'status': 'failed', 'message': 'error occured while registering the brand'}), 500

        else:
            #checking if all the requied field is there

            accepted_poc_payload = ['self', 'name', 'number', 'email', 'designation', 'access', 'password']
            required_poc_payload = accepted_payload

            valid_payload = Helper.check_required_payload(poc_data, accepted_poc_payload, required_poc_payload)

            if valid_payload is not True:
                report = jsonify({'status': 'error', 'message': 'payload does not provide necessary values'}), 400
                # print(report)
                return report
             
            # User is not POC - create new POC with generated user_id
            poc_user_id = User.create_userid()

            # fetch access allower access_specifiers
            access_specifier = Brand.access_specifiers()
            
            # checking if user specified the access
            if poc_data['access'] not in access_specifier or poc_data.get('password')==None:
                return jsonify({'status': 'invalid input', 'message': 'access specifiers are not valid'}), 422

            user_creds={
                'userid': poc_user_id,
                'name': poc_data['name'],
                'number': poc_data['number'],
                'email': poc_data['email'],
                'access': poc_data['access'],
                'designation': poc_data['designation'],
                'hashed_password': User.hash_password(poc_data.get('password'))
                }
            
            await userdb.Write().signup_user(user_creds)
            result = await branddb.Write.insert_brand(brand_id, poc_user_id, brand_data)

            if result == 'failed':
                return jsonify({'status': 'failed', 'message': 'error occured while registering the brand'}), 500

        return jsonify({
            'status': 'ok',
            'message': 'registered the brand successfully'
        }), 201

    except Exception as e:
        print(f'error encountered while registering the brand\n{e}')
        return jsonify({'status': 'error', 'message': 'server error'}), 500


@brand.route('/request-niches', methods=['GET'])
async def request_niches():
    niches = Brand.fetch_niches()
    return jsonify({'status': 'ok', "niches": niches}), 200


# request for brand access
@brand.get('/connect-brand')
@brand.get('/connect-brand/<brand_id>')
@login_required
async def connect_brand(brand_id=None):
    '''
        check the brand access of the user from the database, whether there is any or many or none
    '''

    if brand_id is not None:
        if await branddb.Fetch.check_brand_id(brand_id) == "available":
            session['brand'] = brand_id
            session.permanent = False
            return jsonify({"Status": {"request": "successful", "status": "brand_registered successfully"}})
        else:
            return jsonify({"Status": {"request": "unsuccessful", "status": "invalid brand id"}}), 400

    user_id = session.get('user')
    brand_access = await branddb.Fetch.brand_access(user_id)

    if brand_access is None:
        return jsonify({'Status': {"request": "successful", "brands": None, "status": "not connected", "redirect": "/register-brand"}})
    elif len(brand_access) == 1:
        session['brand'] = brand_access[0].get('brand_id')
        print(f"{session.get('brand')} accessed by {session.get('user')}")
        return jsonify({"Status": {"request": "successful", "brands": "single brand", "status": "connected", "redirect": "/"}})
    else:
        return jsonify({"Status": {"request": "successful", "bands": brand_access, "status": "not connected", "issue": "a brand needs to be selected", "redirect": '/select-panel'}})