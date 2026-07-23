# from quart import current_app, json
# from asyncmy.cursors import DictCursor
# from datetime import datetime


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
