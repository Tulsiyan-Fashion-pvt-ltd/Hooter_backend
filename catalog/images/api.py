from quart import Blueprint, request, session, jsonify, abort, Response
from utils.prerequirements import login_required, brand_required
from . import mariadb
from catalog.products import mariadb as productdb
import asyncio
from utils import imageio
import json
from config import _IMAGE_READ_BUFFER, _IMAGE_WRITE_BUFFER

images = Blueprint("images", __name__, url_prefix = "/images")



@images.post("")
@login_required
@brand_required
async def upload_image():
    args = request.args
    usku_id = args.get("usku-id", type=str)
    if usku_id is None:
        sku_id = args.get("sku-id")
        # print(sku_id)
        is_sku = await productdb.Fetch.is_sku_id_exists(sku_id, session.get("brand"))
        # print(is_sku)
        if is_sku and is_sku.get("found"):
            usku_id=is_sku.get("usku_id")
        else:
            return jsonify({"status": "failed", "msg": "invalid sku id"}), 422
    else:
        '''checking if the usku_id is correct'''
        is_usku_exists = await productdb.Fetch.is_usku_id_exists(usku_id)

        if is_usku_exists != True:
            return jsonify({"status": "invalid usku_id", "msg": 'usku id does not exists'}), 422

    '''VERIFY AND UPLOAD IMAGE'''
    file = await request.files
    form = await request.form

    image_files = file.getlist("image")
    metadata = json.loads(form.get("meta"))

    if not metadata:
        return jsonify({"status": "failed", "message": "metadata is not provided"}), 400
    
    upload_error = []
    try:
        for image_file in image_files:
            print("uploading the image")

            image_name = image_file.filename
            '''checking the file type'''
            check_image = image_name.endswith((".png", ".webp", ".jpeg", ".jpg"))

            if check_image is False:
                upload_error.append({"status": "failed", "message": "file type should be an image"})
                continue

            print(image_name)
            '''CHECKING IF METADATA IS PROVIDED OR NOT'''
            if not metadata.get(image_name):
                upload_error.append({"status": "failed", "message": f"{image_name} meta data for the image is not provided"})
                continue

            '''GET THE IMAGE METADATA'''
            image_type = metadata.get(image_name).get("image_type")
            image_order = metadata.get(image_name).get("image_order")

            if not (image_type and image_order) or (type(image_type) != str or type(image_order) != int):
                upload_error.append({"status": "request failed", "message": f"{image_name} invalid image_type or image_order"})
                continue

            '''GET FILE EXTENSION AND GENERATE FILENAME'''
            image_extended_filename = image_name.split(".")
            image_extension = image_extended_filename[len(image_extended_filename)-1]

            original_image_name = f"{usku_id}/{image_type}.{image_extension}"
            webp_image_name = f"{usku_id}/{image_type}.webp"

            '''adding image entry into the databases'''
            image_path_object = {
                "usku_id": usku_id,
                "url": {"original" :f"catalog/images/original_image/{original_image_name}",
                        "high_resol_webp": f"catalog/images/high_resol_webp/{webp_image_name}",
                        "low_resol_webp": f"catalog/images/low_resol_webp/{webp_image_name}",
                        "webp_card": f"catalog/images/webp_card/{webp_image_name}",
                        },
                "type": image_type,
                "order": image_order 
            }

            result = await asyncio.gather(imageio.write(image_file, image_path_object.get("url")), 
                                        mariadb.Write.image(image_path_object))

            if result[0] == "error" or result[1] != "ok":
                upload_error.append({"status": "failed", "message": f"{image_name} issue occured while uploading the image"})
                continue

    except Exception as e:
        print(f"error occured in upload_image api", e)
        return jsonify({"status": "failed", "message": "internal server error"}), 500

    if len(upload_error) != 0:
        return jsonify({"erros": upload_error}), 422
    return jsonify({"status": "successful", "message": "image uploaded"}), 200


@images.get("")
@login_required
@brand_required
async def get_product_image():
    arguments = request.args

    usku_id = arguments.get("usku-id")
    type = arguments.get("image-type")

    if usku_id == None:
        return jsonify({"status": "failed", "msg": "usku id is not provided"}), 409
    # elif type == None:
    #     return jsonify({"status": "failed", "msg": "image type is is not provided"}), 409

    '''checking the usku_id'''
    if not await productdb.Fetch.is_usku_id_exists(usku_id):
        return jsonify({"status": "failed", "msg": "invalid usku-id"}), 409

    image_urls = await mariadb.Fetch.image(usku_id, type)

    if image_url == "error":
        return jsonify({"status": "failed", "msg": "could not finish the request"}), 500
    elif image_url == None:
        return jsonify({"status": "failed", "msg": "invalid image type"}), 409

    if type is None:
        # print(image_urls)
        image_urls = {value.get("image_type"): {"url": json.loads(value.get("image_url")), "order": value.get("image_order")} for index, value in enumerate(image_urls)}
        return jsonify(image_urls)
    return jsonify(image_urls)


@images.get("/<image_variant>/<filename>")
async def image_url(image_variant: str, filename: str):
    buffer_size = _IMAGE_READ_BUFFER

    mimetype = "image/webp"
    if image_variant == "webp_card":
        filename = f"./.product_images/.image_cards/{filename}"
    elif image_variant == "original":
        split_name = filename.split(".")
        extension = split_name[len(split_name)-1]
        mimetype = f"image/{extension}"
        filename = f"./.product_images/.original_images/{filename}"
        
    elif image_variant == "high_resol_webp":
        filename = f"./.product_images/.high_resol_images/{filename}"
    elif image_variant == "low_resol_webp":
        filename = f"./.product_images/.low_resol_images/{filename}"
    else:
        abort(404)

    image = imageio.read_image_card(filename, buffer_size)
    
    if image is None:
        abort(404)
    
    return Response(image, mimetype=mimetype), 200