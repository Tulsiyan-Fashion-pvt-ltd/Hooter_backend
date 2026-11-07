
from quart import current_app
from asyncmy.cursors import DictCursor


class Fetch:

    @staticmethod
    async def brand_connections(brand_id: str) -> list:
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute("""
                        SELECT 
                            CASE WHEN flipkart.seller_id THEN 'connected' ELSE 'not_connected' END as flipkart, 
                            CASE WHEN shopify.store_id THEN 'connected' ELSE 'not_connected' END as shopify

                            FROM brand
                            LEFT JOIN shopify_stores as shopify 
                                ON shopify.brand_id = brand.brand_id
                            LEFT JOIN flipkart_sellers as flipkart 
                                ON flipkart.brand_id = brand.brand_id
                            WHERE
                                brand.brand_id = %s
                    """, (brand_id,))

                    integrations = await cursor.fetchone()
                    await conn.commit()
                    print(integrations)
                    return{'status': 'ok', 'value': integrations}
                except Exception as e:
                    await conn.rollback()
                    print(f"Error fetching brand integrations: {str(e)}")
                    return {'status': 'failed', 'error': e.args[0]}
