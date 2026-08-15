from PIL import Image, ImageFile
import io
ImageFile.LOAD_TRUNCATED_IMAGES = True


def compress_image_to_low_resol_webp(file: ImageFile) -> io.BytesIO:
    with Image.open(file) as source:
        img = source.convert("RGB")

    return_file = io.BytesIO()

    img.thumbnail((420, 560))
    img.save(
        return_file,
        format="WEBP",
        quality=95,
        optimize=True
    )

    return_file.seek(0)
    return return_file


def compress_main_to_high_resol_webp(file: ImageFile) -> io.BytesIO:
    with Image.open(file) as source:
        img = source.convert("RGB")

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


def compress_main_to_image_card_webp(file: ImageFile) -> io.BytesIO:
    with Image.open(file) as source:
        img = source.convert("RGB")

    return_file = io.BytesIO()

    img.thumbnail((200, 200))
    img.save(
        return_file,
        format="WEBP",
        quality=100,
        optimize=True
    )

    return_file.seek(0)
    return return_file


def convert_into_jpeg(file: ImageFile) -> io.BytesIO:
    with Image.open(file) as source:
        img = source.convert("RGB")

    return_file = io.BytesIO()

    img.thumbnail((800, 800))
    img.save(
        return_file,
        format="JPEG",
        quality=100,
        optimize=True
    )

    return_file.seek(0)
    return return_file