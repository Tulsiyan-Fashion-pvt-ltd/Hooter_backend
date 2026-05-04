from utils import image_compression
import asyncio
import aiofiles
import os

async def write(image: bytes, file_name, bufer_size):
    try:
        '''formating the filename for webp'''
        expended_filename = file_name.split(".")
        file_extension = expended_filename[len(expended_filename)-1]

        webp_extension_file = file_name.replace(f".{file_extension}", ".webp")

        asyncio.create_task(write_original_image(image,file_name))

        asyncio.create_task(write_high_resol_webp(image, webp_extension_file, bufer_size))
        asyncio.create_task(write_low_resol_webp(image, webp_extension_file, bufer_size))
        asyncio.create_task(write_webp_card(image, webp_extension_file, bufer_size))
        return "ok"
    except Exception as e:
        print("could not write the images into the files\n{e}")
        return "error"
    

async def write_original_image(image: bytes, file_name):
    async with aiofiles.open(f"./.product_images/.original_images/{file_name}", "wb") as file:
        await file.write((image))


async def write_high_resol_webp(image: bytes, file_name, bufer_size):
    high_webp_buffer = await asyncio.to_thread(image_compression.compress_main_to_high_resol_webp, image)

    async with aiofiles.open(f"./.product_images/.high_resol_images/{file_name}", "wb") as file:
        while True:
            buffer = await asyncio.to_thread(high_webp_buffer.read, bufer_size)
            if not buffer:
                break
            await file.write((buffer))


async def write_low_resol_webp(image: bytes, file_name, bufer_size):
    low_webp_buffer = await asyncio.to_thread(image_compression.compress_image_to_low_resol_webp, image)

    async with aiofiles.open(f"./.product_images/.low_resol_images/{file_name}", "wb") as file:
        while True:
            buffer = await asyncio.to_thread(low_webp_buffer.read, bufer_size)
            if not buffer:
                break
            await file.write((buffer))


'''writing the low resolution small webp cards after compressing the main image
    they are very useful especially when we really just need a small preview of the product
'''
async def write_webp_card(image: bytes, file_name, bufer_size):
    web_card_buffer = await asyncio.to_thread(image_compression.compress_main_to_image_card_webp, image)

    async with aiofiles.open(f"./.product_images/.image_cards/{file_name}", "wb") as file:
        while True:
            buffer = await asyncio.to_thread(web_card_buffer.read, bufer_size)
            if not buffer:
                break
            await file.write((buffer))



'''
    Read functions for images
'''
async def read_image_card(file_name: str, buffer_size:int):
    try:
        async with aiofiles.open(file_name, "rb") as file:
            while True:
                buffer = await file.read(buffer_size) # defined this value in the app.py file

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