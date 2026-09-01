from quart import current_app
from asyncmy.cursors import DictCursor
from datetime import datetime

class Write:
    @staticmethod
    async def product(product):
        pool = current_app.pool

        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    '''usku_record query'''
                    usku_query = '''insert into usku_record
                                (usku_id, brand_id, sku_id, type_id, type_name)
                                values
                                (%s, %s, %s, %s, %s)
                            '''
                    usku_values = (product.get('usku_id'), product.get('brand_id'), product.get('sku_id')
                                   , product.get("type_id"), product.get('type_name'))
                    
                    await cursor.execute(usku_query, usku_values)

                    '''catalog query'''
                    catalog_query = '''insert into catalog
                                        (usku_id, product_title, product_desc, price,
                                        compared_price, purchasing_cost, vendor, ean, hsn, gtin, upc, isbn, 
                                        net_weight_kg, dead_weight_kg,
                                        volumetric_weight_kg, brand_name)
                                        values
                                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    '''
                    
                    catalog_values = (product.get("usku_id"), product.get("product_title"), product.get("product_desc"),
                                      product.get("price", 0.00), product.get("compared_price", 0.00), product.get("purchasing_cost", 0.00),
                                      product.get("vendor"), product.get("ean"), product.get("hsn"), product.get("gtin"),
                                      product.get("upc"), product.get("isbn"),
                                      product.get("net_weight_kg"), product.get("dead_weight_kg"), product.get("volumetric_weight_kg"),
                                      product.get("brand_name"))

                    await cursor.execute(catalog_query, catalog_values)
                    await connection.commit()
                    return {"error": None}

            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding a single product\n{e}")
                return {"error": e.args[0]}



    @staticmethod
    async def bulk_product(products: list) -> dict:
        print("bulk product working")
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    usku_query = '''insert into usku_record
                                (usku_id, brand_id, sku_id, type_id)
                                values
                                (%s, %s, %s, %s)
                            '''
                    usku_values = [(product.get('usku_id'), product.get('brand_id'), product.get('sku_id')
                                   , product.get("type_id"))
                                    for product in products if products
                                   ]
                    
                    catalog_query = '''insert into catalog
                                        (usku_id, product_title, product_desc, price,
                                        compared_price, purchasing_cost, vendor, ean, hsn, gtin, upc, isbn, 
                                        net_weight_kg, dead_weight_kg,
                                        volumetric_weight_kg, brand_name)
                                        values
                                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    '''
                    
                    catalog_values = [(product.get("usku_id"), product.get("product_title"), product.get("product_desc"),
                                      product.get("price", 0.00), product.get("compared_price", 0.00), product.get("purchasing_cost", 0.00),
                                      product.get("vendor"), product.get("ean"), product.get("hsn"), product.get("gtin"),
                                      product.get("upc"), product.get("isbn"),
                                      product.get("net_weight_kg"), product.get("dead_weight_kg"), product.get("volumetric_weight_kg"),
                                      product.get("brand_name"))
                                        for product in products if products
                                      ]
                    
                    await cursor.executemany(usku_query, usku_values)
                    await cursor.executemany(catalog_query, catalog_values)
                    await connection.commit()
                    return {"error": None}

            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding a bulk product\n{e}")
                return {"error": e.args[0]}


    @staticmethod
    async def variants(usku_id: str, variants: list):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''insert into variants(usku_id, variant_id)
                                values (%s, %s)'''

                    variant_values = [(usku_id, variant.get("variant_id")) for variant in variants]
                    await cursor.executemany(query, variant_values)
                    await connection.commit()
                    return {"error": None}
            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding variants for usku_id {usku_id}", e)
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
    async def update_catalog(catalog: dict):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                        UPDATE usku_record AS u
                        INNER JOIN catalog AS c
                            ON u.usku_id = c.usku_id
                        SET
                            u.sku_id = %s,
                            u.status = %s,
                            c.product_title = %s,
                            c.product_desc=%s,
                            c.price = %s,
                            c.compared_price = %s,
                            c.purchasing_cost = %s,
                            c.vendor = %s,
                            c.ean = %s,
                            c.hsn = %s,
                            c.gtin = %s
                            c.net_weight_kg = %s,
                            c.dead_weight_kg = %s,
                            c.volumetric_weight_kg = %s,
                            c.brand_name = %s
                        WHERE u.usku_id = %s
                    '''

                    values = (
                        catalog.get("sku_id"),
                        "pending",
                        catalog.get("title"),
                        catalog.get("price"),
                        catalog.get("compared_price"),
                        catalog.get("purchasing_cost"),
                        catalog.get("vendor"),
                        catalog.get("ean"),
                        catalog.get("hsn"),
                        catalog.get("net_weight"),
                        catalog.get("dead_weight"),
                        catalog.get("volumetric_weight"),
                        catalog.get("brand_name"),
                        catalog.get("usku_id"),
                    )

                    await cursor.execute(query, values)

                await connection.commit()
                return "ok"

            except Exception as e:
                await connection.rollback()
                print(
                    f"error occured while updating the catalog details of "
                    f"{catalog.get('usku_id')}\n{e}"
                )
                return {"error": e.args[0]}


class Fetch:
    @staticmethod
    async def count_catalogs():
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''select count(usku_id) as count from usku_record'''

                    await cursor.execute(query)
                    count = await cursor.fetchone()
                    return count.get('count') if count else 0
            except Exception as e:
                print(f"error encountered during fetching catalog counts\n{e}")
                return ("error", "error in count_catalogs")


    @staticmethod
    async def is_sku_id_exists(sku_id, brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                
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
    async def is_exists_catalog(brand_id):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
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
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''Select 1 from usku_record where usku_id = %s'''

                    await cursor.execute(query, (usku_id, ))
                    usku = await cursor.fetchone()
                    return True if usku and usku.get('1') else False
            except Exception as e:
                print(f"error occured while fetching the usku_record on is_usku_id_exists function\n{e}")
                return ("error", "could not fetch the availability from the usku_record")

    

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
                    c.product_title, c.compared_price, c.price, c.purchasing_cost, s.status
                    from usku_record as s
                    inner join catalog as c on s.usku_id = c.usku_id
                    left join product_images img on img.usku_id = s.usku_id and
                    img.image_type="front"
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
    async def catalog_upload_count(brand_id: str) -> dict| str:
        """Show the count of uploaded products in the catalog in 
        `pending`, `completed` status and `total` product upload for the brand

        Parameters:
            - brand_id (str) -> brand id

        Returns:
            On Success:
                dict[str, int]:
                    `total`: int
                    `completed`: int
                    `pending`: int    
            On failure:
                - str: `"error"`
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    query = '''
                    select CAST(COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS SIGNED) AS pending,
                    CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS SIGNED) AS completed,
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
