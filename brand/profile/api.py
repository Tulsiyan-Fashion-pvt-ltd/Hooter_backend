"""Brand profile deals with brand data.
These apis offers profile creation, deletion, deta retreival
"""


from quart import Blueprint, request, jsonify, current_app, session
from utils.prerequirements import login_required, brand_required
from security_extensions import validate_csrf
from . import models, services
from pathlib import Path
import asyncio
import secrets
import tempfile
from pathlib import Path
import aiofiles

brand_profile = Blueprint("brand_profile", __name__, url_prefix="/profile")





@brand_profile.get('')
@login_required
@brand_required
async def get_brand_profile():
    """Get brand data  name and profilepic"""
    ...




@brand_profile.put('/image')
@validate_csrf
@login_required
@brand_required
async def upload_brand_profile_image():
    "Upload image for brand profile"
    if request.headers.get('Content-Type') != "application/octet-stream":
        return {'status': 'failed', 'message': 'Octet stream not provided'}, 400
        
    if not request.headers.get('X-Filename'):
        return jsonify({'status': 'failed', 'message': 'X-Filename not provided'}), 400

    filename = request.headers.get('X-Filename')
    suffix = Path(filename).suffix

    if suffix not in (".webp", ".png", ".jpeg", ".jpg"):
        return jsonify({'status': 'failed', 'message': 'Invalid file type'}), 422

    binary_data = await request.data

    # unique_file_identifier = secrets.token_hex(16)
    # temp_filename = f".tempfile/{unique_file_identifier}{suffix}"
    async with aiofiles.tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=suffix) as tempfile:
        await tempfile.write(binary_data)
        temp_filename = tempfile.name
    

     # running the background image compression and upload
    service_response = await services.upload_profile_image_task(session.get('brand'), temp_filename)
    return jsonify(service_response[0]), services[1]