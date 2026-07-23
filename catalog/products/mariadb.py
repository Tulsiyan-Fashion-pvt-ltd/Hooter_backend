from quart import current_app
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
                    
                    catalog_values = (catalog.get("usku_id"), catalog.get("product_title"), 
                                      catalog.get("price"), catalog.get("compared_price"), catalog.get("purchasing_cost"),
                                      catalog.get("vendor"), catalog.get("ean"), catalog.get("hsn"),
                                      catalog.get("net_weight_kg"), catalog.get("dead_weight_kg"), catalog.get("volumetric_weight_kg"),
                                      catalog.get("brand_name"), datetime.now())
                    
                    await cursor.execute(catalog_query, catalog_values)
                    await connection.commit()
                    return "ok"

            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding a single product\n{e}")
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
    async def update_catalog(catalog: dict):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''UPDATE usku_record AS u
                            INNER JOIN catalog AS c 
                                ON u.usku_id = c.usku_id
                            SET 
                                u.sku_id = %s,
                                u.status = %s,
                                c.product_title = %s,
                                c.price = %s,
                                c.compared_price = %s,
                                c.purchasing_cost = %s,
                                c.vendor = %s,
                                c.ean = %s,
                                c.hsn = %s,
                                c.net_weight_kg = %s,
                                c.dead_weight_kg = %s,
                                c.volumetric_weight_kg = %s,
                                c.brand_name = %s,
                                c.updated_at = %s
                                where u.usku_id = %s'''

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
                        datetime.now(),
                        catalog.get("usku_id")
                    )

                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(f"error occured while updating the catalog details of {catalog.get("usku_id")}\n{e}")
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


    