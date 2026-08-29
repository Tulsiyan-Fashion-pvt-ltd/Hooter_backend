_access = ["hooter_admin", "brand_admin", "hooter_member", "brand_member"]
_platforms = ["shopify"]
_FILE_READ_BUFFER = 128 * 1024 # 128 KB 
_FILE_WRITE_BUFFER = 128 * 1024 # 128 KB 

# need to change the access specifer in the files


'''S3 OBJECT STORAGE SERVICE CONFIG'''
import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv
load_dotenv()

_s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("HOOTERS3_ENPOINT_URL"),  # MinIO
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
_product_image_bucket = "product-images"
_product_image_root_key = "catalog/images"

'''IMAGE CONFIGS'''
_image_types = ["original", "high_resol_webp", "low_resol_webp", "webp_card"]