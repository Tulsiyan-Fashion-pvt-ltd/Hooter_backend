from quart import current_app
from asyncmy.cursors import DictCursor
import json

class Write:
    @staticmethod
    async def image(img_obj: dict):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''insert into images(usku_id, image_url, image_type, image_order)
                                values(%s, %s, %s, %s)
                            '''

                    usku_id = img_obj.get("usku_id")
                    image_url = json.dumps(img_obj.get("url"))
                    image_type = img_obj.get("type")
                    image_order = img_obj.get("order")
                    
                    await cursor.execute(query, (usku_id, image_url, image_type, image_order))
                    await connection.commit()
                    return "ok"

            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding a single product\n{e}")
                return {"error": e.args[0]}                          

    @staticmethod
    async def status_complete(usku_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''update usku_record set status="completed"
                            where  usku_id=%s
                            '''
                    values = (usku_id,)

                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                await connection.rollback()
                print(f"error encountered while updating the catalog status as completed\n{e}")
                return {"error": e.args[0]}
            

    @staticmethod
    async def delete_image_all(usku_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''delete from images where usku_id=%s'''
                    values = (usku_id, )

                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(f"error occured while deleting the images of {usku_id}\n{e}")
                return {"error": e.args[0]}



class Fetch:
    @staticmethod
    async def image(usku_id: str, type: str = None):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = ''''''
                    values = ()

                    if type:
                        query = '''select image_url from images where usku_id=%s and image_type=%s'''
                        values = (usku_id, type)
                        await cursor.execute(query, values)
                        urls = await cursor.fetchone()

                        if not urls:
                            return None
                        else:
                            return json.loads(urls.get("image_url"))
                        
                    else:
                        query = '''select image_type, image_url, image_order from images where usku_id=%s'''
                        values = (usku_id, )
                        await cursor.execute(query, values)
                        urls = await cursor.fetchall()
                    
                        if not urls:
                            return None
                        else:
                            return urls
            except Exception as e:
                print(f"error occured while fetching the image urls\n{e}")
                return "error"