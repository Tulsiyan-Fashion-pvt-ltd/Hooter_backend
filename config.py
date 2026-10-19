_access = ["hooter_admin", "brand_admin", "hooter_member", "brand_member"]
_platforms = ["shopify"]
_FILE_READ_BUFFER = 128 * 1024 # 128 KB 
_FILE_WRITE_BUFFER = 128 * 1024 # 128 KB 

# need to change the access specifer in the files


'''S3 OBJECT STORAGE SERVICE CONFIG'''

_product_image_bucket = "product-images"
_brand_image_bucket = "assets"

_product_image_root_key = "catalog/images"
_brand_image_root_key = "/profile"

'''IMAGE CONFIGS'''
_image_types = ["original", "high_resol_webp", "low_resol_webp", "webp_card"]
_max_allowed_image_size = 10 * 1024 * 1024 # 10MB
