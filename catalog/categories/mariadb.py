from quart import current_app, json
from asyncmy.cursors import DictCursor
# from datetime import datetime

class Fetch:
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
    

    @staticmethod
    async def catalog_product(usku_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''select u.sku_id, c.product_title, c.price, c.compared_price,
                    c.purchasing_cost, c.vendor, c.ean, c.hsn, c.net_weight_kg, c.dead_weight_kg, c.volumetric_weight_kg,
                    c.brand_name
                    from usku_record as u
                    inner join catalog as c on u.usku_id=c.usku_id
                    where u.usku_id = %s'''

                    values = (usku_id, )

                    await cursor.execute(query, values)
                    catalog = await cursor.fetchone()
                    return catalog if catalog else {}
            except Exception as e:
                print(f"error occured while fetching the catalog data for {usku_id}\n{e}")
                return {"error": e.args[0]}


    
    @staticmethod
    async def catalog_list(brand_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select COALESCE(JSON_VALUE(img.image_url, "$.webp_card"), '') as image_url, s.usku_id, s.sku_id, 
                    niche.product_name as product_type, niche.type_id,
                    c.product_title, c.compared_price, c.price, c.purchasing_cost, s.status
                    from usku_record as s
                    inner join catalog as c on s.usku_id = c.usku_id
                    left join images img on img.usku_id = s.usku_id and
                    img.image_type="front"
                    inner join niche_products as niche on s.product_type_id = niche.type_id
                    where
                    s.brand_id = %s
                    '''
                    
                    await cursor.execute(query, (brand_id, ))
                    catalog_data = await cursor.fetchall()
                    
                    return catalog_data
            except Exception as e:
                print(f"error occured while fetching the catalog lists\n{e}")
                return "error"
            

    @staticmethod
    async def catalog_upload_count(brand_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''
                    select sum(case when status="pending" then 1 else 0 end) as pending,
                    sum(case when status="completed" then 1 else 0 end) as completed,
                    count(usku_id) as total
                    from usku_record
                    where brand_id = %s
                    '''
                    
                    await cursor.execute(query, (brand_id, ))
                    catalog_data = await cursor.fetchone()
                    return catalog_data
            except Exception as e:
                print(f"error occured while fetching the catalog upload counts\n{e}")
                return "error"



# class Fetch:
        # @staticmethod
    # async def niches():
    #     pool = current_app.pool
    #     async with pool.acquire() as connection:
    #         try:
    #             async with connection.cursor(cursor = DictCursor) as cursor:
    #                 query = '''select niche_id, niche from niches'''

    #                 await cursor.execute(query)
    #                 result = await cursor.fetchall()
                    
    #                 if result:
    #                     return result
    #                 else:
    #                     raise Exception("Could not fetch the niches")
    #         except Exception as e:
    #             print(f"error encountered while fetching the niches in niche_id function\n{e}")
    #             return ("error", "could not fetch the niches")
            
    
    # @staticmethod
    # async def sub_niches(niche_id:int):
    #     pool = current_app.pool
    #     async with pool.acquire() as connection:
    #         try:
    #             async with connection.cursor(cursor = DictCursor) as cursor:
    #                 query = '''select subniche_id, subniche_name from sub_niches where subniche_id like %s'''
    #                 value = (f"{niche_id}%", )

    #                 await cursor.execute(query, value)
    #                 result = await cursor.fetchall()

    #                 if result:
    #                     return result
    #                 else:
    #                     raise Exception("Could not fetch the sub niches")
    #         except Exception as e:
    #             print(f"error encountered while fetching the subniches in sub_niches function\n{e}")
    #             return ("error", "could not fetch the sub_niches")
            
    
    # @staticmethod
    # async def niche_categories(subniche_id:int):
    #     pool = current_app.pool
    #     async with pool.acquire() as connection:
    #         try:
    #             async with connection.cursor(cursor = DictCursor) as cursor:
    #                 query = '''select category_id, category_name from niche_categories where category_id like %s'''
    #                 value = (f"{subniche_id}%", )

    #                 await cursor.execute(query, value)
    #                 result = await cursor.fetchall()

    #                 if result:
    #                     return result
    #                 else:
    #                     raise Exception("Could not fetch the niche categories")
    #         except Exception as e:
    #             print(f"error encountered while fetching the niche_categories\n{e}")
    #             return ("error", "could not fetch the niche-categories")
            
    
    # @staticmethod
    # async def niche_products(category_id:int):
    #     pool = current_app.pool
    #     async with pool.acquire() as connection:
    #         try:
    #             async with connection.cursor(cursor = DictCursor) as cursor:
    #                 query = '''select type_id, product_name from niche_products where type_id like %s'''
    #                 value = (f"{category_id}%", )

    #                 await cursor.execute(query, value)
    #                 result = await cursor.fetchall()

    #                 if result:
    #                     return result
    #                 else:
    #                     raise Exception("Could not fetch the niche products")
    #         except Exception as e:
    #             print(f"error encountered while fetching the niche_products in niche_products function\n{e}")
    #             return ("error", "could not fetch the niche_products")
