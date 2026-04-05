from PIL import Image, ImageFile
import io
ImageFile.LOAD_TRUNCATED_IMAGES = True


def compress_image_to_low_resol_webp(file:ImageFile) -> bytes:
    
    print('running compressiong')
    img = Image.open(io.BytesIO(file)).convert('RGB')   # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((420, 560))
    img.save(return_file, format='WEBP', quality=95, optimize=True)
    return return_file.getvalue()



def compress_main_to_high_resol_webp(file: ImageFile) -> bytes:
    print('running compressiong')
    img = Image.open(io.BytesIO(file)).convert('RGB')    # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((800, 800))
    img.save(return_file, format='WEBP', quality=100, optimize=True)
    return return_file.getvalue()

def compress_main_to_image_card_webp(file: ImageFile) -> bytes:
    print('running compressiong')
    img = Image.open(io.BytesIO(file)).convert('RGB')    # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((200, 200))
    img.save(return_file, format='WEBP', quality=100, optimize=True)
    return return_file.getvalue()


def convert_into_jpeg(file: ImageFile) -> bytes:
    print('running conversion')
    img = Image.open(io.BytesIO(file)).convert('RGB')    # making the file binary object treating as file in the memory
    return_file = io.BytesIO()
    img.thumbnail((800, 800))
    img.save(return_file, format='JPEG', quality=100, optimize=True)
    return return_file.getvalue()