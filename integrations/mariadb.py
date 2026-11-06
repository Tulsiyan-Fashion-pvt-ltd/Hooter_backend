
from quart import current_app
from asyncmy.cursors import DictCursor


class Fetch:

    @staticmethod
    async def get_brand_integrations(brand_id: str) -> list:
        pool = current_app.pool

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute("""
                        SELECT 'flipkart' AS flipkart, flipkart.seller_id, 'shopify' AS shopify, shopify.store_id
                        FROM flipkart_sellers AS flipkart
                        INNER JOIN shopify_stores AS shopify
                            ON flipkart.brand_id = shopify.brand_id
                        WHERE flipkart.brand_id = %s
                    """, (brand_id,))

                    integrations = await cursor.fetchall()
                    return integrations

                except Exception as e:
                    print(f"Error fetching brand integrations: {str(e)}")
                    return None
