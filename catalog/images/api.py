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
from ..products.authorize import product_api_access_required

images = Blueprint("images", __name__, url_prefix = "/images")



@images.post("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def upload_image(usku_id):
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



@images.get("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def get_image_url(usku_id):
    """Gets the image urls
    """
    arguments = request.args
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

    image = await asyncio.to_thread(read_object, _product_image_bucket, key)
    
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