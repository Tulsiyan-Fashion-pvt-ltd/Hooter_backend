from PIL import Image, ImageFile
import io
ImageFile.LOAD_TRUNCATED_IMAGES = True


def low_resol_webp(object: bytes) -> io.BytesIO:
    img = Image.open(io.BytesIO(object)).convert('RGB')   # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((420, 560))
    img.save(return_file, format='WEBP', quality=95, optimize=True)
    return_file.seek(0)
    return return_file


def high_resol_webp(object: bytes) -> io.BytesIO:
    img = Image.open(io.BytesIO(object)).convert("RGB")

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


def image_card_webp(object: bytes) -> io.BytesIO:
    img = Image.open(io.BytesIO(object)).convert('RGB')    # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((200, 200))
    img.save(return_file, format='WEBP', quality=100, optimize=True)
    return_file.seek(0)
    return return_file


def convert_into_jpeg(object: bytes) -> io.BytesIO:
    img = Image.open(io.BytesIO(object)).convert('RGB')    # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((800, 800))
    img.save(return_file, format='JPEG', quality=100, optimize=True)
    return_file.seek(0)
    return return_file