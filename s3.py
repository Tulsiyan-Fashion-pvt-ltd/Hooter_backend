import os
from werkzeug.datastructures import FileStorage
from io import BytesIO
import traceback
from botocore.exceptions import ClientError
import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv
from typing import Literal

load_dotenv()


_s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("HOOTERS3_ENDPOINT_URL"),  # MinIO
    aws_access_key_id=os.environ.get("HOOTERS3_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("HOOTERS3_SECRET_ACCESS_KEY"),
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={
            "addressing_style": "path"
        }
    )
)



def upload_object(bucket: str, key: str, object: bytes = None, file: FileStorage = None)-> Literal["ok"]| Literal["error"]:
    """
    :UPLOAD OBJECT FILE:
    Uploads object file to s3 server

    :Arguments:
    bucket -> bucket name,
    object -> bytes,
    key -> object key,

    :Returns:
    "ok" -> on success
    "error" -> on failure
    """
    try:
        file = BytesIO(object) if object else file  #making copy of the image
        _s3.upload_fileobj(
            file,
            bucket,
            key
        )
        return "ok"
    except Exception as e:
        traceback.print_exc()
        print(f"error encountered while uploading object {key} to the object bucket {bucket}\n{e}")
        return "error"




def read_object(bucket: str, key: str):
    """
    Read object from amazon s3 or minio
    
    
    Arguments:
        bucket: bucket name, 
        key: object key (/product/key/object.jpg)
    
    Returns:
        bytes -> generator  on success:
        {"error", error_key(int)}-> on error
    """
    try:
        response = _s3.get_object(
            Bucket=bucket,
            Key=key
        )

        body = response.get("Body")
        data = body.read()
        return data
    except ClientError as e:
        print(f"error encountered while reading object {key} from bucket {bucket}\n{e}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"error": 404}
    



def delete_object(bucket: str, key: str):
    """Delete object key from the bucket
    
    Parameters:
        bucket: bucket name,
        key: object key (/product/key/object.jpg)
    
    Returns:
    "ok" -> on success:
    "error" -> on error
    """
    try:
        _s3.delete_object(
            Bucket=bucket,
            Key=key
        )

        return "ok"
    except Exception as e:
        traceback.print_exc()
        print(f"error encountered while deleting the object {key} from the bucket\n{e}")
        return "error"



def delete_bulk_objects(bucket: str, keys: list) -> str:
    """Delete objects from s3 like object storage in bulk
    
    Parameters:
        bucket: bucket name
        keys: list of keys of the objects

    Returns:
        "ok" -> on success:
        "error" -> on failure
    """
    try:
        response = _s3.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": [
                        {"Key": key}
                        for key in keys
                    ]
                }
            )

        if response.get("Errors"):
            return "error"
        else: "ok"
    except Exception as e:
        traceback.print_exc()
        print(f"error occured while deleting batch objects\ndelete_bulk_objects\n", e)
        return "error"











if __name__ == "__main__":
    response = _s3.list_buckets()

    for bucket in response["Buckets"]:
        print(bucket["Name"])