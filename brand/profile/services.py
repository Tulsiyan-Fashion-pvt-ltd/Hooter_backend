from werkzeug.datastructures import FileStorage
import tempfile
from pathlib import Path
from ..auth.authorize import brand_access_required
from utils.image_compression import image_card_webp
import asyncio
import aiofiles
import s3
from config import _brand_image_bucket, _brand_image_root_key
from . import mariadb
from quart import url_for

async def save_temp_image(image: FileStorage):
    """Save image to a temporary file to use it later on.
    It uses tempfile so after using the file, use image.unlink
    
    Args:
        image: filestore image file

    Returns:
        filename: PathLike string    
    """
    with tempfile.NamedTemporaryFile(suffix=Path(image.filename).suffix, delete=False) as temp_file:
        file_name = temp_file.filename
        await image.save(file_name)
        return file_name



@brand_access_required
async def upload_profile_image_task(brand_id: str, image_path: Path) -> tuple[dict[str, str], int]:
    """Runs compressions and uploads the compressed image to the s3 like buket
    
    Args:
        brand_id: 
            Unique ID for brand

        image_path:
            Path string for image
    
    Returns:
        tuple with dict object and api status code
    """
    try:
        compressed_image_file = await asyncio.to_thread(image_card_webp, image_path=image_path)
        Path(image_path).unlink(missing_ok=True)  # removing tempfile is ultra important
    except Exception as e:
        print(e)
        return {"code": "9a4b457b", "message":  "Image compression failed", "status": "failed"}, 500


    '''upload to s3 and store key in the db'''
    suffix = Path(image_path).suffix
    s3_image_key = f"{_brand_image_root_key}/{brand_id}{suffix}"
    task_response = await asyncio.gather(asyncio.to_thread(s3.upload_object, _brand_image_bucket, s3_image_key, file=compressed_image_file),
                         mariadb.Write.profile_img_key(brand_id, s3_image_key))

    [s3_response, sql_response] = task_response

    if s3_response != "ok" or sql_response.get('status') != 'ok':
        return {'status': 'failed', 'message': 'Could not upload the profile image', 'code': '7de0788e'}, 500
    elif sql_response.get('status') !="ok":
        await s3.delete_object(_brand_image_bucket, s3_image_key)  # optimistic deletion
        return {'status': 'failed', 'message': 'Could not upload the profile image', 'code': 'd9d560e3'}, 500
    elif s3_response != "ok":
        await mariadb.Write.delete_profile_img_key(brand_id)        # optimistic deletion
        return {'status': 'failed', 'message': 'Could not upload the profile image', 'code': 'b6dc22f7'}, 500
    else:
        return {'status': 'successful', 'message': 'uploaded successfully'}, 200
    


@brand_access_required
async def get_brand_profile(brand_id: str) -> tuple[dict[str, str], int]:
    """Get brand profile
    
    Args:
        brand_id: Unique brand ID

    Returns:
        tuple with dict object and rest status api code
    """
    response = await mariadb.Fetch.brand_profile(brand_id)
    if not response:
        return {'status': 'failed', 'message': 'Unexpected error'}, 500
    elif response.get('error'):
        return {'status': 'failed', 'message': 'Could not fetch the brand profile', 'code': response.get('code')}, 500

    # replace the image key with the image url
    image_key  = response.pop('profile_img_key')
    response['image_url'] = f"{url_for('brand.brand_profile.get_brand_profile_image', key=image_key)}" if image_key else None
    return response, 200
