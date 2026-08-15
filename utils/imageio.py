from utils import image_compression
import asyncio
import aiofiles
import os
from config import _product_image_bucket
from werkzeug.datastructures import FileStorage
from io import BytesIO
import s3



async def write(image: FileStorage, url: dict):
    try:
        '''formating the filename for webp'''
        asyncio.create_task(asyncio.to_thread(s3.upload_object, _product_image_bucket, image, url.get('original')))
        asyncio.create_task(write_high_resol_webp(image, url.get("high_resol_webp")))
        asyncio.create_task(write_low_resol_webp(image, url.get("low_resol_webp")))
        asyncio.create_task(write_webp_card(image, url.get("webp_card")))
        return "ok"
    except Exception as e:
        print(f"could not write the write the images\n{e}")
        return "error"
    
    

async def write_high_resol_webp(image: FileStorage, url:dict):
    high_webp_buffer = await asyncio.to_thread(image_compression.compress_main_to_high_resol_webp, image)
    await asyncio.to_thread(s3.upload_object, _product_image_bucket, high_webp_buffer, url)



async def write_low_resol_webp(image: FileStorage, url:dict):
    low_webp_buffer = await asyncio.to_thread(image_compression.compress_image_to_low_resol_webp, image)
    await asyncio.to_thread(s3.upload_object, _product_image_bucket, low_webp_buffer, url)



'''writing the low resolution small webp cards after compressing the main image
    they are very useful especially when we really just need a small preview of the product
'''
async def write_webp_card(image: FileStorage, url:dict):
    web_card_buffer = await asyncio.to_thread(image_compression.compress_main_to_image_card_webp, image)
    await asyncio.to_thread(s3.upload_object, _product_image_bucket, web_card_buffer, url)



async def read_image(file_name: str):
    """
    :DESCRIPTION:
    Read image from the s3

    :Aruguments:
    file_name -> object key


    :Returns:
    generator bytes
    """
    try:
        async with aiofiles.open(file_name, "rb") as file:
            while True:
                buffer = await file.read() # defined this value in the app.py file

                if not buffer:
                    break
                yield buffer
    except Exception as e:
        print(f"could not read the image\n{e}")
        return
    

'''delete images'''
async def delete_image(file_path: str):
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File {file_path} deleted successfully.")
            "ok"
        else:
            print(f"The file {file_path} does not exist.")
            "finished"
    except Exception as e:
        print(f"could not read the image\n{e}")
        return "error"