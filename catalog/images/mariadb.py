from quart import current_app
from asyncmy.cursors import DictCursor
import json

class Write:
    @staticmethod
    async def image(img_obj: dict) -> dict:
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    usku_id = img_obj.get("usku_id")
                    image_urls = img_obj.get("url")
                    image_type = img_obj.get("type")
                    image_order = img_obj.get("order")
                    
                    '''Inserting record in product_images'''
                    query = '''insert into product_images(usku_id, image_type,  image_order)
                                values(%s, %s, %s)
                            '''
                    values = (usku_id, image_type, image_order)
                    await cursor.execute(query, values)

                    id = cursor.lastrowid
                    '''Adding relational data to image_urls'''
                    query = '''insert into image_urls (image_id, image_variation, image_url)
                            values(%s, %s, %s)'''
                    values = [(id, key, image_urls.get(key))   for key in image_urls]
                    await cursor.executemany(query, values)

                    await connection.commit()
                    return {"response": "ok", "error": None}

            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding a single product\n{e}")
                return {"error": e.args[0]}                          
            

class Delete:
    @staticmethod
    async def image(usku_id: str, image_type: str) -> dict["error": str | None]:
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = f'''delete from product_images where usku_id=%s 
                            and image_type = %s
                            '''
                    values = (usku_id, image_type)

                    await cursor.execute(query, values)
                    await connection.commit()
                    return {"error": None}
            except Exception as e:
                print(f"error occured while deleting the images of {usku_id}\n{e}")
                return {"error": e.args[0]}


    @staticmethod
    async def all_image(usku_id: str) -> dict[str, str]| dict[str, None]:
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = f'''delete from product_images where usku_id=%s 
                            '''
                    values = (usku_id, )

                    await cursor.execute(query, values)
                    await connection.commit()
                    return {"error": None}
            except Exception as e:
                print(f"error occured while deleting the images of {usku_id}\n{e}")
                return {"error": e.args[0]}


class Fetch:
    @staticmethod
    async def image(usku_id: str, type: str = None) -> dict[str, str]| list[dict[str, str| int]]| str:
        """Fetch image urls from for the given usku id that 
        later can be use to download the images from the s3 server.

        Arguments:
            usku_id: universal sku id of the product:
            type: image type of the url e.g. front | back| zoomed etc
        
        Returns:
            dict["{image_resolution}", "{image_url}"]: 
                if the the image `type` is provided then it returns a dict containing 
                the image resolution as key and `image key` for s3 bucket

            list[dict[str, str| int]]: 
                if the image `type` is not provided then it returns a `list` containing a `dict`
                of keys as `image_type -> str`, `image_url -> json.dumps(resol, s3-keys)`,
                `image_order`

            error: on failure
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    if type:
                        query = '''select url.image_variation, url.image_url, image.image_order from 
                            product_images as image
                            join 
                            image_urls as url
                            on
                            url.image_id = image.id
                             where image.usku_id=%s and image.image_type=%s'''
                        values = (usku_id, type)
                        await cursor.execute(query, values)
                        urls = await cursor.fetchall()

                        
                    else:
                        query = '''select image.image_type, url.image_variation, url.image_url, image.image_order from 
                            product_images as image
                            join 
                            image_urls as url
                            on
                            url.image_id = image.id
                             where image.usku_id=%s'''
                        values = (usku_id, )
                        await cursor.execute(query, values)
                        urls = await cursor.fetchall()
                    
                    return urls
            except Exception as e:
                print(f"error occured while fetching the image urls\n{e}")
                return "error"