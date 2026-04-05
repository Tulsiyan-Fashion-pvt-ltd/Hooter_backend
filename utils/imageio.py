from quart import current_app
from utils import image_compression
import asyncio
import aiofiles
from io import BytesIO

async def write(image: bytes, file_name):
    async with aiofiles.open(f"./.product_images/.original_images/{file_name}", "wb") as file:
        await file.write((image))

    asyncio.create_task(write_original_image, image,file_name)
    asyncio.create_task(write_high_resol_webp(image, file_name))
    asyncio.create_task(write_low_resol_webp(image, file_name))
    asyncio.create_task(write_webp_card(image, file_name))
    return "ok"
    

'''writing the original image and adding into the sql record'''
async def write_original_image(image: bytes, file_name):
    async with aiofiles.open(f"./.product_images/.original_images/{file_name}", "wb") as file:
        await file.write((image))


'''writing the high resolution webp after compressing the main image'''
async def write_high_resol_webp(image: bytes, file_name):
    high_webp = await asyncio.to_thread(image_compression.compress_main_to_high_resol_webp, image)

    async with aiofiles.open(f"./.product_images/.high_resol_images/{file_name}", "wb") as file:
        await file.write((high_webp))


'''writing the low resolution webp after compressing the main image'''
async def write_low_resol_webp(image: bytes, file_name):
    high_webp = await asyncio.to_thread(image_compression.compress_image_to_low_resol_webp, image)

    async with aiofiles.open(f"./.product_images/.low_resol_images/{file_name}", "wb") as file:
        await file.write((high_webp))

'''writing the low resolution small webp cards after compressing the main image
    they are very useful especially when we really just need a small preview of the product
'''
async def write_webp_card(image: bytes, file_name):
    high_webp = await asyncio.to_thread(image_compression.compress_main_to_image_card_webp, image)

    async with aiofiles.open(f"./.product_images/.image_cards/{file_name}", "wb") as file:
        await file.write((high_webp))


async def read_image_card(file_name: str):
    async with aiofiles.open(f"./.product_images/.image_cards/{file_name}", "rb") as file:
        while True:
            buffer = await file.read(current_app.config["IMAGE_READ_BUFFER"]) # defined this value in the app.py file

            if not buffer:
                break
            yield buffer