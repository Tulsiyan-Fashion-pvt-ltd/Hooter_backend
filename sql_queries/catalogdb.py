from quart import current_app, json
from asyncmy.cursors import DictCursor
from datetime import datetime

class Write:
    @staticmethod
    async def catalog(catalog):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    usku_query = '''insert into usku_record
                                (usku_id, brand_id, sku_id, product_type_id)
                                values
                                (%s, %s, %s, %s)
                            '''
                    usku_values = (catalog.get('usku_id'), catalog.get('brand_id'), catalog.get('sku_id')
                                   , catalog.get("type_id"))
                    
                    await cursor.execute(usku_query, usku_values)
                    catalog_query = '''insert into catalog
                                        (usku_id, product_title, price,
                                        compared_price, purchasing_cost, vendor, ean, hsn, net_weight_kg, dead_weight_kg,
                                        volumetric_weight_kg, brand_name, updated_at)
                                        values
                                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    '''
                    
                    catalog_values = (catalog.get("usku_id"), catalog.get("title"), 
                                      catalog.get("price"), catalog.get("compared_price"), catalog.get("purchasing_cost"),
                                      catalog.get("vendor"), catalog.get("ean"), catalog.get("hsn"),
                                      catalog.get("net_weight"), catalog.get("dead_weight"), catalog.get("volumetric_weight"),
                                      catalog.get("brand_name"), datetime.now())
                    
                    await cursor.execute(catalog_query, catalog_values)
                    await connection.commit()
                    return "ok"

            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding a single product\n{e}")
                return {"error": e.args[0]}
            

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



class Fetch:
    @staticmethod
    async def count_catalogs():
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor = DictCursor) as cursor:
                try:
                    query = '''select count(usku_id) as count from usku_record'''

                    await cursor.execute(query)
                    count = await cursor.fetchone()
                    return count.get('count') if count else 0
                except Exception as e:
                    print(f"error encountered during fetching catalog counts\n{e}")
                    return ("error", "error in count_catalogs")


    # this function only tells if the brand has uploaded a single catalog or not            
    @staticmethod
    async def is_exists_catalog(brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''Select 1 from usku_record where brand_id = %s'''

                    await cursor.execute(query, (brand_id, ))
                    catalog_available = await cursor.fetchone()
                    return True if catalog_available and catalog_available.get('1') else False
                except Exception as e:
                    print(f"error occured while fetching the catalog on is_exists_catalog function\n{e}")
                    return ("error", "could not fetch the availability from the usku_record")
                
    @staticmethod
    async def is_usku_id_exists(usku_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''Select 1 from usku_record where usku_id = %s'''

                    await cursor.execute(query, (usku_id, ))
                    usku = await cursor.fetchone()
                    return True if usku and usku.get('1') else False
                except Exception as e:
                    print(f"error occured while fetching the usku_record on is_usku_id_exists function\n{e}")
                    return ("error", "could not fetch the availability from the usku_record")
                
    
    @staticmethod
    async def niches():
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select niche_id, niche from niches'''

                    await cursor.execute(query)
                    result = await cursor.fetchall()

                    return "failed" if not result else result
            except Exception as e:
                print(f"error encountered while fetching the niches in niche_id function\n{e}")
                return ("error", "could not fetch the niches")
            
    
    @staticmethod
    async def sub_niches(niche_id:int):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select subniche_id, subniche_name from sub_niches where subniche_id like %s'''
                    value = (f"{niche_id}%", )

                    await cursor.execute(query, value)
                    result = await cursor.fetchall()

                    return "failed" if not result else result
            except Exception as e:
                print(f"error encountered while fetching the subniches in sub_niches function\n{e}")
                return ("error", "could not fetch the sub_niches")
            
    
    @staticmethod
    async def niche_categories(subniche_id:int):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select category_id, category_name from niche_categories where category_id like %s'''
                    value = (f"{subniche_id}%", )

                    await cursor.execute(query, value)
                    result = await cursor.fetchall()

                    return "failed" if not result else result
            except Exception as e:
                print(f"error encountered while fetching the niche_categories\n{e}")
                return ("error", "could not fetch the niche-categories")
            
    
    @staticmethod
    async def niche_products(category_id:int):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select type_id, product_name from niche_products where type_id like %s'''
                    value = (f"{category_id}%", )

                    await cursor.execute(query, value)
                    result = await cursor.fetchall()

                    return "failed" if not result else result
            except Exception as e:
                print(f"error encountered while fetching the niche_products in niche_products function\n{e}")
                return ("error", "could not fetch the niche_products")


    @staticmethod
    async def image(usku_id: str, type: str):
        print(usku_id, type)
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select image_url from images where usku_id=%s and image_type=%s'''
                    values = (usku_id, type)

                    await cursor.execute(query, values)
                    urls = await cursor.fetchone()
                    if not urls:
                        return None
                    else:
                        return json.loads(urls.get("image_url"))
            except Exception as e:
                print(f"error occured while fetching the image urls\n{e}")
                return "error"