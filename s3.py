import os
from config import _s3, _product_image_bucket, _FILE_READ_BUFFER
from werkzeug.datastructures import FileStorage
from io import BytesIO



def upload_object(bucket: str, file_object: FileStorage, key: str)-> str:
    """
    :UPLOAD OBJECT FILE:
    Uploads object file to s3 server

    :Arguments:
    bucket -> bucket name
    file_object -> file storage
    key -> object key

    :Returns:
    "ok" -> on success
    "error" -> on failure
    """
    try:
        image = BytesIO(image.read()) #making copy of the image
        _s3.upload_fileobj(
            file_object,
            bucket,
            key
        )
    except Exception as e:
        print(f"error encountered while uploading object {key} to the object bucket {bucket}\n{e}")
        return "error"




def read_object(bucket: str, key: str):
    """
    :READ OBJECT FROM S3:
    Read object from amazon s3 or minio
    
    :Arguments:
    bucket -> bucket name
    key -> object key (/product/key/object.jpg)
    
    :Returns:
    generator  bytes -> on success
    "error" -> on error
    """
    try:
        response = _s3.get_object(
            Bucket=bucket,
            Key=key
        )

        body = response.get("Body")
        data = body.read(_FILE_READ_BUFFER)
        while data:
            yield data
    except Exception as e:
        print(f"error encountered while reading object {key} from bucket {bucket}\n{e}")
        return "error"
    



def delete_object(bucket: str, key: str):
    """:DELTE OBJECT FROM S3:
    Delete object key from the bucket

    :Arguments:
    bucket -> bucket name
    key -> object key (/product/key/object.jpg)
    
    :Returns:
    "ok" -> on success
    "error" -> on error
    """

    try:
        _s3.delete_object(
            Bucket=bucket,
            Key=key
        )

        return "ok"
    except Exception as e:
        print(f"error encountered while deleting the object {key} from the bucket\n{e}")
        return "error"



if __name__ == "__main__":
    response = _s3.list_buckets()

    for bucket in response["Buckets"]:
        print(bucket["Name"])