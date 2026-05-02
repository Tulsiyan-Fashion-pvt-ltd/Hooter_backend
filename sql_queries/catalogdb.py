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
                                        (NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), 
                                        NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), 
                                        NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), 
                                        NULLIF(%s, ''))
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
    async def delete_catalog(usku_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''delete from usku_record
                                where usku_id=%s
                            '''
                    values = (usku_id, )

                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except  Exception as e:
                await connection.rollback()
                print(f"error encountered while deleting the product {usku_id} from the catalog\n{e}")
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
    async def is_sku_id_exists(sku_id, brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            async with connection.cursor(cursor=DictCursor) as cursor:
                try:
                    query = '''select 1 as found, usku_id from usku_record where sku_id=%s and brand_id=%s'''
                    values = (sku_id, brand_id)

                    await cursor.execute(query, values) 
                    sku = await cursor.fetchone()
                    
                    if sku:
                        return sku
                    else:
                        return {}
                except Exception as e:
                    print(f"error occured while fetching the sku_id from the brand {brand_id}\n{e}")
                    return None

    @staticmethod
    async def niches():
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select niche_id, niche from niches'''

                    await cursor.execute(query)
                    result = await cursor.fetchall()
                    
                    if result:
                        return result
                    else:
                        raise Exception("Could not fetch the niches")
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

                    if result:
                        return result
                    else:
                        raise Exception("Could not fetch the sub niches")
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

                    if result:
                        return result
                    else:
                        raise Exception("Could not fetch the niche categories")
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

                    if result:
                        return result
                    else:
                        raise Exception("Could not fetch the niche products")
            except Exception as e:
                print(f"error encountered while fetching the niche_products in niche_products function\n{e}")
                return ("error", "could not fetch the niche_products")


    @staticmethod
    async def image(usku_id: str, type: str):
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
                        query = '''select image_type, image_url from images where usku_id=%s'''
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