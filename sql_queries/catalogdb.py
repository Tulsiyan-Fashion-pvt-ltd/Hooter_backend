from quart import current_app
from asyncmy.cursors import DictCursor

class Write:
    @staticmethod
    async def add_single_catalog(catalog):
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
                                          catalog.get("price"), catalog.get("comp_price"), catalog.get("purchasing_cost"),
                                          catalog.get("vendor"), catalog.get("ean"), catalog.get("hsn"),
                                          catalog.get("net_weight"), catalog.get("dead_weight"), catalog.get("volumetric_weight"),
                                          catalog.get("brand_name"), catalog.get('update_timestamp'))
                        
                        await cursor.execute(catalog_query, catalog_values)
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