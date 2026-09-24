from quart import session
from brand.auth.authorize import brand_access_required
from brand.auth import mariadb
from .utils import create_id
from users.authentication.services import signup_user


async def connect_brand(user_id: str) -> dict[str, str| int]:
    """Connects the brand session with the use session
    
    Args:
        - user_id - Unique Id for user
        
    Returns:
        dict with status `code`,  `status`, `brands`, `connection` and `message` keys
    """
    brand_access = await mariadb.Fetch.brand_access(user_id)
    if brand_access is None:
        return {"status": "successful", "brands": None, "connection": "not connected", "message": "no brand is registered", "code": 201}, 
    elif len(brand_access) == 1:
        session['brand'] = brand_access[0].get('brand_id')
        return {"status": "successful", "brands": brand_access, "connection": "connected", "message": "brand connected successfully", "code": 200}
    else:
        return {"status": "successful", "bands": brand_access, "connection": "not connected", "message": "a brand needs to be selected", 'code': 201}



@brand_access_required
async def connect_brand_id(brand_id: str) -> dict[str, str| int]:
    """Connects the brand session of given brand_id with the use session
    
    Args:
        - brand_id - Unique ID for brands
        
    Returns:
        dict with status `code`, `status` and `message` keys
    """
    session['brand'] = brand_id
    return {"status": "successful", "message": "brand registered successfully", 'code': 200}



async def register_brand(user_id: str, brand_data: dict) -> dict[str, str| int]:
    '''Register the brand for the given user

    Args:
        - user_id - Unique ID for the user
        - brand_data - dict object with brand payload
    Returns:
        dict with status, message, brand_id and code
    '''
    brand_data = {"brand_id": create_id(), **brand_data}
    result = await mariadb.Write.insert_brand(brand_data, user_id, access="brand_admin")

    if result != "ok":
        if result == 1265:
            return {"status": "failed", "message": "Invalid value provided"}
        elif result == 1062:
            return {'status': 'failed', 'message': 'brand already exists', 'code': 409}
        else:
            return {'status': 'failed', 'message': 'error occured while registering the brand', 'code': 500}
    else:
        return {'status': 'successful', 'message': 'Successfully registered the brand', 
                'brand_id': brand_data.get('brand_id'), 'code': 200}


@brand_access_required
async def add_new_poc(brand_id: str, poc_data: str):
    '''Update the brand Point Of Contact
    
    Args: 
        - brand_id - Unique brand ID
        - user_id - Unique ID for the user
        
    Returns:

    '''
    '''sign up the new user for poc'''
    signup_response = await signup_user(poc_data)
    if signup_response.get('code') != 200:
        return signup_response

    '''map poc user to brand'''
    poc_id = signup_response.get('userid')
    access_query = await mariadb.Write.map_user_brand(brand_id, poc_id, 'brand_member')
    if access_query != 'ok':
        if access_query == 1062:
            return {'status': 'failed', 'message': 'User already belongs to the brand', 'code': 409}
        else:
            return {'status': 'error', 'message': 'Error occured while adding the brand poc', 'code': 500}

    '''update poc for brand'''
    update_resposne = await mariadb.Write.update_poc(brand_id, poc_id)
    if update_resposne != "ok":
        return {'status': 'error', 'message': 'Could not update the poc but the poc has registered', 'code': 201}

    return {'status': 'successful', 'message': 'Successfully added the new user as poc', 'code': 200}
