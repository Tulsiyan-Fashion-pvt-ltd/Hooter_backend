from quart import Blueprint, request, session, jsonify, abort, Response, url_for
from utils.prerequirements import login_required, brand_required
from utils.helper import Payload
from . import mariadb
from catalog.products import mariadb as productdb
from catalog.categories import mongodb as categories_mongodb
import json
from s3 import read_object
from config import _product_image_bucket, _product_image_root_key, _image_types, _max_allowed_image_size
from . import services
import asyncio
from ..products.authorize import product_api_access_required
from uuid import uuid4
from .sse import tasks, image_sse
from traceback import print_exc
import os
from dotenv import load_dotenv

load_dotenv()

images = Blueprint("images", __name__, url_prefix = "/images")
images.register_blueprint(image_sse)


@images.post("/<usku_id>")
@login_required
@brand_required
@product_api_access_required
async def upload_image(usku_id):
    '''VERIFY AND UPLOAD IMAGE'''
    files = await request.files
    form = await request.form
    type_id = request.args.get('type-id')

    try:
        metadata = json.loads(form.get("meta"))
    except Exception as e:
        print(e)
        print_exc()
        return jsonify({'status': 'failed', 'message': 'invalid stringify json'}), 400

    if not metadata:
        return jsonify({"status": "failed", "message": "metadata is not provided"}), 400

    if not type_id:
        return jsonify({"status": "failed", "message": "type-id is not provided"}), 400
    
    img_mandatory_keys = await categories_mongodb.Fetch.Attributes.Images(type_id).mandatory()

    if not Payload.check_required_payload(metadata, img_mandatory_keys):
        return jsonify({'status': 'failed', 'message': 'mandatory image attributes not provided'}), 400
    
    '''Saving the files in temps'''
    image_dict_object = {}      # stores image `path`, `order` on key `image_type`
    for key in metadata.keys():
        file = files.get(key) # getting the file from files by their keys e.i front, zoomed etc

        '''Checking the file size'''
        stream = file.stream
        pos = stream.tell()
        stream.seek(0, 2)          # end
        file_size = stream.tell()        # bytes
        stream.seek(pos)            # restore position
        if file_size >= _max_allowed_image_size:
            return jsonify({'status': 'denied', 'message': 'Image too large', "allowed_size": "10MB"}), 413

        '''checking the file type'''
        image_name = file.filename
        check_image = image_name.endswith((".png", ".webp", ".jpeg", ".jpg"))    
        if check_image is False:
            return jsonify({"status": "failed", "message": "File type should be an image", "image_type": key}), 413

        if  0 >= metadata.get(key):
            return jsonify({'status': "failed", 'message': "Image order can not be equal to or less then 0"}), 413

        '''Else store in the temp path for background upload'''
        path = await services.save_image_temp(file)
        image_dict_object[key] = {"path": path, "order": metadata.get(key)} # metadata stores image_type as key and image_order as value


    """UPLOAD UNIT SO IT CAN BE RUN ASYNCHRONOUSLY ON THE IMAGE DATA"""
    try:
        job_id = uuid4().hex
        bg_task = asyncio.create_task(services.background_upload_bulk_images(job_id, image_dict_object, usku_id))
        bg_task.job_id = job_id
        bg_task.add_done_callback(services.on_bg_image_upload_task_done)
        tasks[job_id] = {"task": bg_task, 
                         "event": asyncio.Event(),
                         "progress": "0%"}
    
    except Exception as e:
        print(f"error occured in upload_image api", e)
        print_exc()
        return jsonify({"status": "failed", "message": "Internal server error"}), 500

    return jsonify({"status": "successful", 
                    "message": "Image upload has started", 
                    "event_url": url_for("catalog.images.image_sse.images_upload_sse", job_id=job_id)}), 200



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