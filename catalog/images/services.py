import s3
from utils import image_compression
import asyncio
from config import _product_image_bucket, _product_image_root_key
from werkzeug.datastructures import FileStorage
from . import mariadb
from ..products import mariadb as product_sql
import tempfile
from pathlib import Path
import aiofiles
from traceback import print_exc
from .sse import tasks
from pathlib import Path
import logging
import json

async def image_variants_upload(image: bytes, url: dict) -> str:
    """Create and upload additional image variants to the S3-compatible server.

    The function generates three image variants from the original image:
    high-resolution WebP, low-resolution WebP, and card-sized WebP, and
    uploads each variant to its corresponding S3 object key.

    Args:
        image: The original image as bytes.
        url: A dictionary containing the S3 object keys for each image variant.

    Returns:
        "ok" if all image variants are successfully created and uploaded.\n
        "error" if an error occurs during processing or uploading.
    """
    try:
        def compress_high_upload(image:bytes, key: str):
            compressed_image = image_compression.high_resol_webp(image)
            s3.upload_object(_product_image_bucket, key, file=compressed_image)



        def compress_low_upload(image:bytes, key: str):
            compressed_image = image_compression.low_resol_webp(image)
            s3.upload_object(_product_image_bucket, key, file=compressed_image)



        def compress_card_upload(image:bytes, key: str):
            compressed_image = image_compression.image_card_webp(image)
            s3.upload_object(_product_image_bucket, key, file=compressed_image)


        await asyncio.gather(
            asyncio.to_thread(s3.upload_object, _product_image_bucket, url.get("original"), object=image),
            asyncio.to_thread(compress_high_upload, image, url.get("high_resol_webp")),
            asyncio.to_thread(compress_low_upload, image, url.get("low_resol_webp")),
            asyncio.to_thread(compress_card_upload, image, url.get("webp_card"))
        )

        return "ok"
    except Exception as e:
        print(f"error occourced while making coppies and uploading the image from image services", e)
        return "error"


async def save_image_temp(file: FileStorage) -> str:
    """Saves the file into a temporary path and returns the path
    
    Args:
        file: Filestorage object
        
    Returns:
        str:
            temp path ex /temp/xysjs.jpeg
    """    
    suffix = Path(file.filename).suffix
    print("suffix >>>", suffix)
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    path = temp.name
    temp.close()

    await file.save(path)
    return path



async def upload_image_type(image_file: FileStorage| str, image_type: str, image_order: int, usku_id: str, ):
    """Upload image object and image meta data to the databases
    
    Args:
        image_file: FileStoage object containing the image bytes or image_file path
        image_type: String value of image type
        image_order: int value of image order
        usku_id: str

    Returns:
        None
    """
    if type(image_file) == str:       # if image_file is a path
        image_name = Path(image_file).name
        async with aiofiles.open(image_file, "rb") as file:
            image = await file.read()
    else:
        image_name = image_file.filename
        image = image_file.read()

    if not (image_type and image_order) or (type(image_type) != str or type(image_order) != int):
        raise Exception("Invalid image_type or image_order")

    '''GET FILE EXTENSION AND GENERATE FILENAME'''
    image_extension = Path(image_name).suffix
    original_image_name = f"{usku_id}/{image_type}{image_extension}"
    webp_image_name = f"{usku_id}/{image_type}.webp"

    '''adding image entry into the databases'''
    image_path_object = {
        "usku_id": usku_id,
        "url": {"original" :f"{_product_image_root_key}/original_image/{original_image_name}", # this url is the key for the s3
                "high_resol_webp": f"{_product_image_root_key}/high_resol_webp/{webp_image_name}",
                "low_resol_webp": f"{_product_image_root_key}/low_resol_webp/{webp_image_name}",
                "webp_card": f"{_product_image_root_key}/webp_card/{webp_image_name}",
                },
        "type": image_type,
        "order": image_order 
    }

    print(image_path_object.get('url'))

    sql_data_response = await mariadb.Write.image(image_path_object)
    if sql_data_response.get('error') == 1062:
        raise Exception("Duplicate image")
    elif sql_data_response.get("error"):
        raise Exception("Issue occured while image data")
    
    s3_upload_response = await image_variants_upload(image, image_path_object.get("url"))
    if s3_upload_response == "error":
        raise Exception("Could not import image")
    return None



async def background_upload_bulk_images(job_id: str, image_data: dict, usku_id: str):
    """Takes the image file paths and image meta deta and upload them using `upload_image_type` one by one
    
    Args:
        job_id:
            Unique job ID for the background task
        image_data:
            dict of `image_types` as keys with `order` and `path` as their dict value
        usku_id:
            Uique universal ID of Stock Keeping Unit
    
    Returns:
        dict:
            `status`, `message` and `error` key for the upload status. If the status is successful then the images
            uploaded successfully and error is None and if status is failed the error will be `str`
    """

    upload_report = {} # image_types as keys and dict response and value
    keys = image_data.keys()

    '''Progress reporting for background job'''
    total_work = len(keys)
    work_done = 0
    for key in keys:       # key is image_type
        image_path = image_data.get(key).get("path")
        try:
            await upload_image_type(image_path, key, image_data.get(key).get('order'), usku_id)
        except Exception as e:
            print_exc()
            logging.exception(e)
            upload_report[key] = {"status": "failed", "message": e.args[0]}

        else:
            upload_report[key] = {"status": "successful", "message": "Image uploaded successfully"}
            work_done += 1
            tasks[job_id]['progress'] = f"{round(((work_done/total_work)*100), 2)}%"
            tasks[job_id]['event'].set() # execute the waiting function

        finally:
            Path(image_path).unlink()

    if work_done == total_work:
        await product_sql.Write.status_complete(usku_id)
    return upload_report



def on_bg_image_upload_task_done(task):
    """Need to set the event as True to run it after the bg func has finished"""
    job_id = task.job_id
    event = tasks.get(job_id).get('event')
    event.set()




async def delete_images(usku_id:str, image_type:str| None = None):
    """Delete the images from the rdbms awa from the s3 like object storage service
    
    Parameters:
        usku_id: univeral sku id of the product
        image_type: (optional) type of the image e.g. front, back, zoomed etc

    Returns:
        dict containing error and message
        
        error:
            failed | None
        message:
            descriptive message
    """
    urls = await mariadb.Fetch.image(usku_id, image_type)

    if urls == "error":
        return {"error": "failed", "message": "Unable to fetch the urls for the product"}

    if not image_type:
        keys = [
                image_url
                for url in urls
                for image_url in json.loads(url.get("image_url", {})).values()
            ] if urls else []
    else:
        keys = json.loads(urls.get("image_url")).values() if urls and urls.get("image_url") else []

    if [] == keys:
        return {"error": "failed", "message": "Product does not have images to delete"}

    s3_responses = await asyncio.to_thread(s3.delete_bulk_objects, _product_image_bucket, keys)
    if "error" == s3_responses:
        return {"error": "failed", "message": "Unable to delete all the images"}

    sql_response = await mariadb.Delete.image(usku_id, image_type) if image_type else await mariadb.Delete.all_image(usku_id)
    if sql_response.get("error"):
        return {"error": "failed", "message": "Deleted the images but could not delete the records"}

    return {"error": None, "message": "Successfully deleted the images"}
    