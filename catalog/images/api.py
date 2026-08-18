from quart import Blueprint, request, session, jsonify, abort, Response
from utils.prerequirements import login_required, brand_required
from . import mariadb
from catalog.products import mariadb as productdb
import json
from s3 import read_object
from config import _product_image_bucket, _product_image_root_key, _image_types
from . import services
from werkzeug.datastructures import FileStorage
import asyncio


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
    

    """UPLOAD UNIT SO IT CAN BE RUN ASYNCHRONOUSLY ON THE IMAGE DATA"""

    try:
        upload_status = await asyncio.gather(
            *(services.upload_unit(image_file, metadata, usku_id) for image_file in image_files)
        )
    except Exception as e:
        print(f"error occured in upload_image api", e)
        return jsonify({"status": "failed", "message": "internal server error"}), 500

    if all("failed" == upload.get("status") for upload in upload_status):
        return jsonify({"erros": upload_status}), 422
    return jsonify({"status": "successful", "message": "image uploaded"}), 200



@images.get("")
@login_required
@brand_required
async def get_image_url():
    """Gets the image urls
    """
    arguments = request.args

    usku_id = arguments.get("usku-id")
    type = arguments.get("image-type")

    if usku_id == None:
        return jsonify({"status": "failed", "msg": "usku id is not provided"}), 409
    

    '''checking the usku_id'''
    if not await productdb.Fetch.is_usku_id_exists(usku_id):
        return jsonify({"status": "failed", "msg": "invalid usku-id"}), 409

    image_urls = await mariadb.Fetch.image(usku_id, type)

    if image_urls == "error":
        return jsonify({"status": "failed", "msg": "could not finish the request"}), 500
    elif image_urls == None:
        return jsonify({"status": "failed", "msg": "invalid image type"}), 409

    if type is None:
        print(image_urls)
        image_urls = {value.get("image_type"): {"url": json.loads(value.get("image_url")), "order": value.get("image_order")} for  value in image_urls}
        return jsonify(image_urls)
    
    return jsonify(image_urls)




@images.get("/<image_variant>/<usku_id>/<image>")
async def get_image(image_variant: str, usku_id: str, image: str):
    key = f"{_product_image_root_key}/{image_variant}/{usku_id}/{image}"

    if (image_variant not in _image_types) and (usku_id is None) and (image is None):
        return jsonify({"status": "bad request", "message": "arguments not provided correctly"}), 400

    mimetype = "image/webp" # default it's webp 
    if image_variant == "original":
        split_name = image.split(".")
        extension = split_name[len(split_name)-1]
        mimetype = f"image/{extension}" # if the original image is requested then the mimetype is changed

    image = read_object(_product_image_bucket, key)
    
    if image is None:
        abort(404)
    
    return Response(image, mimetype=mimetype), 200



@images.delete("/<usku_id>")
@images.delete("/<usku_id>/<image_type>")
async def delete(usku_id: str, image_type: str = None):
    if not usku_id :
        return jsonify({"status": "bad request", "message": "usku id is not provided"}), 400

    response = await services.delete_images(usku_id, image_type)

    if response.get("error"):
        return jsonify(response), 500

    return jsonify({"status": "successful", "message": "successfully deleted the images"}), 200