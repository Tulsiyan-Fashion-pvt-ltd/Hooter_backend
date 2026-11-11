from quart import current_app
from asyncmy.cursors import DictCursor
from .helper import create_variant_id
from traceback import print_exc
from typing import Literal
from os import getenv
from dotenv import load_dotenv
from . import helper

load_dotenv()



class Write:
    @staticmethod
    async def variants(parent_usku_id: str, usku_ids: list) -> dict[str, str|int]:
        """
        Relate multiple proucts for a specific variant ID

        Args:
            parent_usku_id: usku_id for the reference product
            usku_ids: USKU IDs to upload as a variant

        Returns:
            dict with `status`, `value`(variant_id) and `error`
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''SELECT variant_id
                                FROM product_variants
                                WHERE usku_id = %s'''
                    values = (parent_usku_id, )
                    await cursor.execute(query, values)
                    variant_id = await cursor.fetchone()

                    if not variant_id:
                        variant_id = helper.create_variant_id()
                        usku_ids.append(parent_usku_id) # if the parent isn't already there then add the parent too for linking
                    else:
                        variant_id = variant_id.get('variant_id')

                    query = '''
                    INSERT INTO 
                        product_variants(variant_id, usku_id)
                    VALUES
                        (%s, %s)
                    '''
                    values = [(variant_id, usku_id) for usku_id in usku_ids]
                    await cursor.executemany(query, values)
                    await connection.commit()

                    return {'status': 'ok', 'value': variant_id}
            except Exception as e:
                await connection.rollback()
                print(f"error encountered while adding variants for id", e)
                return {'status': 'failed', "error": e.args[0]}




class Fetch:
    @staticmethod
    async def variant_id(usku_id: str) -> dict[str, str|int]:
        """
        Fetches the variant Id for the given USKU ID

        Args:
            usku_id: Universally unique SKU ID
        
        Returns:
            dict with status, result, and error as keys
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            SELECT variant_id
                            FROM product_variants
                            WHERE usku_id = %s
                            '''
                    value = (usku_id, )
                    await cursor.execute(query, value)
                    result = await cursor.fetchone()
                    await connection.commit()

                    variant_id = result.get('variant_id') if result else None
                    return {"status": 'ok', "value": variant_id}                    
            except Exception as e:
                await connection.rollback()
                print(e)
                print_exc()
                return {"error": e.args[0], 'status': 'failed'}


    @staticmethod
    async def count_variants(usku_id: str) -> dict[str, str|int]:
        """
        Returns the count of the variants of the product

        Args:
            usku_id: Universally unique SKU ID
        
        Returns:
            dict with status, result, and error as keys
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            SELECT COUNT(usku_id) as count
                            FROM product_variants
                            WHERE variant_id = (SELECT variant_id 
                                FROM product_variants
                                WHERE usku_id = %s)
                            '''
                    value = (usku_id, )
                    await cursor.execute(query, value)
                    count = await cursor.fetchone()
                    await connection.commit()

                    return {"status": 'ok', "value": count.get('count') if count else None}                    
            except Exception as e:
                await connection.rollback()
                print(e)
                print_exc()
                return {"error": e.args[0], 'status': 'failed'}


    @staticmethod
    async def variants(usku_id: str) -> dict[Literal['status', 'error', 'value'], any]:
        """
        Returns the listing information for all the variants
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = f'''SELECT CONCAT('{getenv('APP_DOMAIN')}/',image_urls.image_url) as main_image, 
                    u.usku_id, u.sku_id, u.type_name, u.created_at, u.status, 
                    c.product_title, c.product_desc, c.price, c.compared_price,
                    c.purchasing_cost, c.vendor, c.ean, c.hsn, c.gtin, c.upc, c.isbn,
                    c.net_weight_kg, c.dead_weight_kg, c.volumetric_weight_kg, 
                    c.brand_name, c.updated_at
                    FROM 
                        product_variants as variants
                    INNER JOIN
                        usku_record as u
                        ON u.usku_id = variants.usku_id
                    INNER JOIN 
                        catalog as c on u.usku_id=c.usku_id
                    LEFT JOIN 
                        product_images AS images
                        ON images.usku_id=u.usku_id
                        AND images.image_type = 'main'
                    LEFT JOIN 
                        image_urls
                        ON image_urls.image_id = images.id
                        AND image_urls.image_variation='webp_card'
                    where variants.variant_id = (SELECT variant_id FROM product_variants WHERE usku_id = %s)'''

                    value = (usku_id, )
                    await cursor.execute(query, value)
                    result = await cursor.fetchall()
                    await connection.commit()

                    return {"status": 'ok', "value": result}                    
            except Exception as e:
                await connection.rollback()
                print(e)
                print_exc()
                return {"error": e.args[0], 'status': 'failed'}