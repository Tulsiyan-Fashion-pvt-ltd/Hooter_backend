import asyncio
from quart import current_app, json
import asyncmy
from asyncmy.cursors import DictCursor
from datetime import datetime

class Write:

    '''create the inward and mark it pending'''
    @staticmethod
    async def inward(inward_data: dict, brand_id: str) -> str:
        """
        Creates an inward and returns the inward id if the inward id is not provided
        Updates the iwnard data if the inward_id is provided

        If an inwrad id is provided then the func updates the stock info of that inward id

        Args:
            inward_data (dict: ) 
                keys = (supplier_id, usku_ids (list or dictionaries)| keys = (usku_id, expected)})
            brand_id (str)
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:

                    if not inward_data.get("inward_id"):
                        query = '''
                                    insert into inward(brand_id, supplier_id, po_num, created_at)
                                    values(%s, %s, %s)
                                '''
                        values = (brand_id, inward_data.get("supplier_id"), inward_data.get("po"), datetime.now())

                        await cursor.execute(query, values)
                        inward_id = cursor.lastrowid

                        for unit in inward_data.get("usku_ids", []):
                            query = '''
                                        insert into inward_items(inward_id, usku_id, expected_qtt, received_qtt, 
                                        shortage, overage, rejected)
                                        values(%s, %s, %s, %s, %s, %s, %s)
                                    '''

                            values = (inward_id, unit.get("usku_id"), unit.get("expected"), unit.get("recieved", 0), 
                                      unit.get("shortage", 0), unit.get("overage", 0), unit.get("rejected", 0))
                            await cursor.execute(query, values)
                        await connection.commit()
                        return inward_id        
                                
                    else:
                        status = inward_data.get("status")
                        inward_id = inward_data.get("inward_id")
                        if not status:
                            raise Exception("inward status is missing") 

                        query = '''
                                update inward set
                                inward_status = %s,
                                updated_at = %s
                                where inward_id = %s
                                '''
                        values = (status, datetime.now(), inward_data.get("inward_id"))

                        await cursor.execute(query, values)

                        for unit in inward_data.get("usku_ids", []):
                            query = '''
                                    update inward_items set
                                    received_qtt = %s,
                                    shortage= %s,
                                    overage=%s,
                                    rejected=%s where
                                    inward_id = %s and usku_id = %s 
                                    '''
                            values = (unit.get("recieved", 0), unit.get("shortage", 0), 
                                      unit.get("rejected", 0), inward_data.get("inward_id"), unit.get("usku_id"))
                            
                            await cursor.execute(query, values)

                        '''create the grn record'''
                        prefix = "GRN"
                        year = datetime.now().date().year
                        count = 0 

                        query = '''
                                select count(grn_id) as count from grn where inward_id = %s
                                '''
                        values = (inward_id, )
                        count = await cursor.execute(query, values)

                        count = count.get("count")
                        grn_id = f"{prefix}-{year}-{inward_id}{count}"

                        '''create grn record'''
                        query = '''insert into grn(grn_id, inward_id, created_at)
                                values(%s, %s, %s)
                                '''
                        values = (grn_id, inward_id, datetime.now())

                        await cursor.execute(query, values)
                        await connection.commit()
                        return grn_id
            except Exception as e:
                print(f"error occured while creating inward\n{e}")
                await connection.rollback()
                return "error"


    @staticmethod
    async def supplier(data: dict): 
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into supplier(name, contact_number, address, email)
                                values(%s, %s, %s, %s)
                            '''
                    values = (data.get("name"), data.get("number"), data.get("address"), data.get("email"))
                    
                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(f"error encountered while adding supplier\n{e}")
                await connection.rollback()
                return "error"
            
    @staticmethod
    async def grn(data: dict):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into grn(inward_id, created_at)
                                values(%s, %s)
                            '''
                    values = (data.get("inward_id"), datetime.now())

                    await cursor.execute(query, values)
                    await connection.commit()
                    return "ok"
            except Exception as e:
                print(f"error enountered while adding grn record\n{e}")
                await connection.rollback()
                return "error"




class Fetch:
    @staticmethod
    async def inventory(usku_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''select u.usku_id, u.sku_id, n.product_name as product_type, c.product_stock
                                from usku_record as u
                                inner join catalog as c on u.usku_id = c.usku_id
                                inner join niche_products as n on u.product_type_id = n.type_id
                                where u.usku_id=%s and status="completed"
                            '''
                    values = (usku_id, )

                    await cursor.execute(query, values)
                    inventory = await cursor.fetchall()
                    return inventory
            except Exception as e:
                print(f"error encountered whie fetching the inventory for {usku_id}\n{e}")
                return "error"

    @staticmethod
    async def inward(condition: str, brand_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                                select inward_id, created_at, updated_at
                                from inward
                                where inward_status = %s and brand_id=%s
                            '''
                    values = (condition, brand_id)

                    await cursor.execute(query, values)
                    inwards = await cursor.fetchall()

                    '''add the items in the inward details as well'''
                
                    for index, inward in enumerate(inwards):
                        inward_id = inward.get("inward_id", None)
                        query = '''
                                    select i.usku_id, u.sku_id, i.expected_qtt, i.received_qtt, i.shortage, i.overage, i.rejected
                                    from inward_items as i
                                    join usku_record as u on i.usku_id = u.usku_id
                                    where
                                    inward_id= %s
                                '''
                        values = (inward_id, )
                        await cursor.execute(query, values)

                        items = await cursor.fetchall()

                        inwards[index]["items"] = items if items else {}

                    return inwards
            except Exception as e:
                print(f"error encountered whie fetching the inward for {brand_id}\n{e}")
                return "error"
            

    @staticmethod
    async def suppliers(brand_id: str):
        """
        RETURNS THE SUPPLIERS OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select supplier_id, name, contact_number, address, email
                            from supplier where
                            brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)

                    suppliers = await cursor.fetchall()

                    suppliers = [{
                        "supplier_id": supplier.get("supplier_id"),
                        "name": supplier.get("name"),
                        "number": supplier.get("contact_number"),
                        "email": supplier.get("email"),
                        "address": json.loads(supplier.get("address"))
                    } for supplier in suppliers if suppliers]

                    return suppliers
            except Exception as e:
                print(f"error occured while fetching the suppliers of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}

    @staticmethod
    async def grn_count(inward_id):
        """
        FETCHES THE NUMBER OF GRNS FOR THE PROVIDED INWARD ID
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select count(grn_id) as count from grn where inward_id = %s
                            '''
                    values = (inward_id, )
                    count = await cursor.execute(query, values)
                    await connection.commit()
                    return count.get("count") if count else "error"
            except Exception as e:
                print(f"error occured while fetching the grn count for the inward {inward_id}\n{e}")
                return {"error", e.args[0]}