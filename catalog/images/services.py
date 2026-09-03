import s3
from utils import image_compression
import asyncio
from config import _product_image_bucket, _product_image_root_key
from werkzeug.datastructures import FileStorage
from . import mariadb


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




async def upload_unit(image_file: FileStorage, metadata: dict, usku_id: str, ):
    """Upload image object and image meta data to the databases
    
    Parameters:
        image_file: FileStoage object containing the image bytes,
        metadata: dict object containing the meta-data for the image
            file_name{
                image_order: str,
                image_type: int
            }
        usku_id: str

    Returns:
        status: str,
        message: str
    """
    image_name = image_file.filename
    '''checking the file type'''
    check_image = image_name.endswith((".png", ".webp", ".jpeg", ".jpg"))

    if check_image is False:
        return {"status": "failed", "message": "file type should be an image"}

    '''CHECKING IF METADATA IS PROVIDED OR NOT'''
    if not metadata.get(image_name):
        return {"status": "failed", "message": f"{image_name} meta data for the image is not provided"}


    '''GET THE IMAGE METADATA'''
    image_type = metadata.get(image_name).get("image_type")
    image_order = metadata.get(image_name).get("image_order")

    if not (image_type and image_order) or (type(image_type) != str or type(image_order) != int):
        return {"status": "request failed", "message": f"{image_name} invalid image_type or image_order"}

    '''GET FILE EXTENSION AND GENERATE FILENAME'''
    image_extended_filename = image_name.split(".")
    image_extension = image_extended_filename[len(image_extended_filename)-1]

    original_image_name = f"{usku_id}/{image_type}.{image_extension}"
    webp_image_name = f"{usku_id}/{image_type}.webp"

    '''adding image entry into the databases'''
    image_path_object = {
        "usku_id": usku_id,
        "url": {"original" :f"{_product_image_root_key}/original_image/{original_image_name}",
                "high_resol_webp": f"{_product_image_root_key}/high_resol_webp/{webp_image_name}",
                "low_resol_webp": f"{_product_image_root_key}/low_resol_webp/{webp_image_name}",
                "webp_card": f"{_product_image_root_key}/webp_card/{webp_image_name}",
                },
        "type": image_type,
        "order": image_order 
    }

    image = image_file.read()

    s3_response = await image_variants_upload(image, image_path_object.get("url"))


    sql_response = await mariadb.Write.image(image_path_object) if "error" != s3_response else None

    if "error" == s3_response or sql_response.get("error"):
        return {"status": "failed", "message": f"{image_name} issue occured while uploading the image"}

    return {"status": "successful", "message": f"{image_name} uploaded to the minio s3 compatible server"}



async def background_upload_bulk_images(imageFiles: list):
    """Takes the image files and image meta deta and upload them using `upload_unit` one by one"""
    ...



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
        return {"error": "failed", "message": "unable to fetch the urls for the product"}

    if not image_type:
        keys = [
                image_url
                for url in urls
                for image_url in url.get("image_url", {}).values()
            ] if urls else []
    else:
        keys = urls.values() if urls else []

    print(keys)

    s3_responses = await asyncio.to_thread(s3.delete_bulk_objects, _product_image_bucket, keys)

    if "error" == s3_responses:
        return {"error": "failed", "message": "unable to delete all the images"}

    sql_response = await mariadb.Delete.image(usku_id, image_type) if image_type else await mariadb.Delete.all_image(usku_id)
    if sql_response.get("error"):
        return {"error": "failed", "message": "deleted the images but could not delete the records"}

    return {"error": None, "message": "successfully deleted the images"}
    