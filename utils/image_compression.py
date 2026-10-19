from PIL import Image, ImageFile
import io
from werkzeug.datastructures import FileStorage
from pathlib import Path
ImageFile.LOAD_TRUNCATED_IMAGES = True


def low_resol_webp(image: bytes| FileStorage = None, image_path: Path = None, copy: bool = True) -> io.BytesIO:
    if image_path and type(image_path) == str:
        with open(image_path, 'rb') as imagefile:
            img = Image.open(imagefile).convert('RGB')
    elif image:  
        if copy == True:
            img = Image.open(io.BytesIO(image)).convert('RGB')    # making the file binary image treating as file in the memory
        else:
            img = Image.open(image).convert('RGB')
    return_file = io.BytesIO()
    img.thumbnail((420, 560))
    img.save(return_file, format='WEBP', quality=95, optimize=True)
    return_file.seek(0)
    return return_file


def high_resol_webp(image: bytes| FileStorage = None, image_path: Path = None, copy: bool = True) -> io.BytesIO:
    if image_path and type(image_path) == str:
        with open(image_path, 'rb') as imagefile:
            img = Image.open(imagefile).convert('RGB')
    elif image:  
        if copy == True:
            img = Image.open(io.BytesIO(image)).convert('RGB')    # making the file binary image treating as file in the memory
        else:
            img = Image.open(image).convert('RGB')

    return_file = io.BytesIO()
    img.thumbnail((800, 800))
    img.save(
        return_file,
        format="WEBP",
        quality=100,
        optimize=True
    )

    return_file.seek(0)
    return return_file


def image_card_webp(image: bytes| FileStorage = None, image_path: Path = None, copy: bool = True) -> io.BytesIO:
    if image_path and type(image_path) == str:
        with open(image_path, 'rb') as imagefile:
            # print(img.read())
            img = Image.open(imagefile).convert('RGB')
    elif image:  
        if copy == True:
            img = Image.open(io.BytesIO(image)).convert('RGB')    # making the file binary image treating as file in the memory
        else:
            img = Image.open(image).convert('RGB')

    return_file = io.BytesIO()
    img.thumbnail((200, 200))
    img.save(return_file, format='WEBP', quality=100, optimize=True)
    return_file.seek(0)
    return return_file


def convert_into_jpeg(image: bytes| FileStorage = None, image_path: Path = None, copy: bool = True) -> io.BytesIO:
    if image_path and type(image_path) == str:
        with open(image_path, 'rb') as imagefile:
            img = Image.open(imagefile).convert('RGB')
    elif image:  
        if copy == True:
            img = Image.open(io.BytesIO(image)).convert('RGB')    # making the file binary image treating as file in the memory
        else:
            img = Image.open(image).convert('RGB')

    return_file = io.BytesIO()
    img.thumbnail((800, 800))
    img.save(return_file, format='JPEG', quality=100, optimize=True)
    return_file.seek(0)
    return return_file