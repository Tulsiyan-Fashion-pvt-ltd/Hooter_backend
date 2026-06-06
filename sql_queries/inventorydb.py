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
                                    insert into inward(brand_id, supplier_id, warehouse_id, created_at)
                                    values(%s, %s, %s, %s)
                                '''
                        values = (brand_id, inward_data.get("supplier_id"), inward_data.get("warehouse_id"),  datetime.now())

                        await cursor.execute(query, values)
                        inward_id = cursor.lastrowid

                        for usku_id, obj in inward_data.get("usku_ids", {}).items():
                            query = '''
                                        insert into inward_items(inward_id, usku_id, po_num, expected_qtt, received_qtt, 
                                        shortage, overage, rejected, uom)
                                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    '''

                            values = (inward_id, usku_id, obj.get("po"), obj.get("exp_stock"), obj.get("recieved", 0), 
                                      obj.get("shortage", 0), obj.get("overage", 0), obj.get("rejected", 0), obj.get("uom"))
                            await cursor.execute(query, values)


                        shipment = inward_data.get("shipment")
                        query = '''
                                insert into shipment(inward_id, shipment_ref_no, vehicle_no, transporter, 
                                delivery_challan, arrival_date)
                                values(%s, %s, %s, %s, %s, %s)
                                '''
                        values = (inward_id, shipment.get("shipment-ref"), shipment.get("vehicle-no"), 
                                  shipment.get("transporter"), shipment.get("challan"), shipment.get("arrival-date"))
                        
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
                                insert into supplier(name, brand_id, contact_number, address, email, created_at)
                                values(%s, %s, %s, %s, %s, %s)
                            '''
                    values = (data.get("name"), data.get("brand_id"), data.get("number"), data.get("address"), 
                              data.get("email"), datetime.now())
                    
                    await cursor.execute(query, values)
                    supplier_id = cursor.lastrowid

                    await connection.commit()
                    return supplier_id
            except Exception as e:
                print(f"error encountered while adding supplier\n{e}")
                await connection.rollback()
                return "error"
            

    @staticmethod
    async def warehouse(data: dict): 
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor() as cursor:
                    query = '''
                                insert into warehouse(name, brand_id, phone, address, email)
                                values(%s, %s, %s, %s, %s)
                            '''
                    values = (data.get("name"), data.get("brand_id"), data.get("number"), data.get("address"), data.get("email"))
                    
                    await cursor.execute(query, values)
                    warehouse_id = cursor.lastrowid
                    await connection.commit()
                    return warehouse_id
            except Exception as e:
                print(f"error encountered while adding warehouse\n{e}")
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
    async def inventory(brand_id: str, filter: str = "", usku_id: str = None):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    sql_condition = ""
                    if filter == "sellable":
                        sql_condition = "and c.product_stock != 0"
                    elif filter == "oos":
                        sql_condition = "and c.product_stock = 0"
                    elif filter == "low-stock":
                        sql_condition = "and c.product_stock <= 10 and c.product_stock > 0"   

                    query = f'''select img.image_url, u.usku_id, u.sku_id, c.product_title, n.product_name as product_type, c.product_stock
                                from usku_record as u
                                inner join catalog as c on u.usku_id = c.usku_id
                                inner join niche_products as n on u.product_type_id = n.type_id
                                inner join images as img on u.usku_id = img.usku_id
                                where u.brand_id = %s and u.status="completed" and img.image_type = "front"
                                and {"u.usku_id = %s" if usku_id else "1=1"}
                                {sql_condition}
                            '''
                    # print(query)
                    values = (brand_id, usku_id) if usku_id else (brand_id, )

                    await cursor.execute(query, values)
                    inventory = await cursor.fetchall()
                    return inventory
            except Exception as e:
                print(f"error encountered whie fetching the inventory for {brand_id}\n{e}")
                return "error"


    @staticmethod
    async def stock_count(brand_id: str):
        """
        FETCHES THE STOCK COUNT OF SELLABLE, OOS AND LOW STOCK PRODUCTS
        """
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select
                            count(c.product_stock) as total, 
                            count(case when c.product_stock = 0 then 1 end) as oos,
                            count(case when c.product_stock!=0 then 1 end) as sellable,
                            count(case when c.product_stock <= 10 and c.product_stock >0 then 1 end) as low
                            from catalog as c
                            inner join usku_record as u on c.usku_id = u.usku_id
                            where u.brand_id = %s and u.status="completed"
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    stock = await cursor.fetchone()
                    return stock if stock else {}
            except Exception as e:
                print(f"error encountered whie fetching the stock count from inventory for {brand_id}\n{e}")
                return "error"

    
    @staticmethod
    async def inward_count(brand_id: str):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''select 
                                count(inward_id) as total,
                                count(case when inward_status = "completed" then 1 end) as completed,
                                count(case when inward_status="partial" then 1 end) as partial,
                                count(case when inward_status="pending" then 1 end) as pending
                                from inward
                                where brand_id=%s
                    '''
                    values = (brand_id, )
                    await cursor.execute(query, values)
                    count = await cursor.fetchone()
                    return count if count else {}
            except Exception as e:
                print(f"error encountered whie fetching the inward count for {brand_id}\n{e}")
                return "error"
    
    @staticmethod
    async def inward(condition: str, brand_id: str, inward_id: str = None):
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    if inward_id:
                        # fetch the specified inward for the logged in brand
                        query = '''select created_at from inward where inward_id = %s and brand_id = %s'''
                        values = (inward_id, brand_id)

                        await cursor.execute(query, values)
                        date = await cursor.fetchone()

                        query = '''select img.image_type, img.image_url, u.sku_id, c.product_title, niche.product_name as product_type, 
                                inward_items.uom, inward_items.expected_qtt
                                from
                                inward
                                inner join inward_items on inward.inward_id = inward_items.inward_id
                                inner join usku_record as u on inward_items.usku_id = u.usku_id
                                inner join catalog as c on u.usku_id = c.usku_id
                                inner join images as img on u.usku_id = img.usku_id
                                inner join niche_products as niche on u.product_type_id = niche.type_id
                                where inward.inward_id = %s and inward.brand_id = %s and 
                                (inward.inward_status != "cancelled" or inward.inward_status != "completed")
                                and img.image_type="front"
                        '''
                        values = await cursor.execute(query, values)
                        inward_items = await cursor.fetchall() 

                        if not date or not inward_items:
                            return "error"
                        else:
                            return {"created_at": date, "uskus": inward_items}

                    ''' if the inward_id is not provided rather brand_id and condition is provided'''
                    if condition == "completed":
                        condition = '''inward.inward_status = "completed"'''
                    elif condition == "partial":
                        condition = '''inward.inward_status = "partial"'''
                    elif condition == "pending":
                        condition = '''inward.inward_status = "pending"'''
                    elif condition == None:
                        condition = "1=1"

                    query = f'''
                                select inward.inward_id, inward.created_at, inward.updated_at, s.name as supplier
                                from inward
                                inner join supplier as s on inward.supplier_id=s.supplier_id
                                where inward.brand_id=%s and {condition}
                            '''
                    values = (brand_id)

                    await cursor.execute(query, values)
                    inwards = await cursor.fetchall()

                    # '''add the items in the inward details as well'''
                
                    # for index, inward in enumerate(inwards):
                    #     inward_id = inward.get("inward_id", None)
                    #     query = '''
                    #                 select i.usku_id, u.sku_id, i.expected_qtt, i.received_qtt, i.shortage, i.overage, i.rejected
                    #                 from inward_items as i
                    #                 join usku_record as u on i.usku_id = u.usku_id
                    #                 where
                    #                 inward_id= %s
                    #             '''
                    #     values = (inward_id, )
                    #     await cursor.execute(query, values)

                    #     items = await cursor.fetchall()

                    #     inwards[index]["items"] = items if items else {}

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
    async def supplier(brand_id: str, supplier_id: str):
        """
        RETURNS THE SUPPLIERS OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select name, contact_number, address, email
                            from supplier where
                            brand_id = %s and supplier_id = %s
                            '''
                    values = (brand_id, supplier_id)
                    await cursor.execute(query, values)

                    supplier = await cursor.fetchone()

                    supplier = {
                        "name": supplier.get("name"),
                        "number": supplier.get("contact_number"),
                        "email": supplier.get("email"),
                        "address": json.loads(supplier.get("address"))
                    }

                    return supplier
            except Exception as e:
                print(f"error occured while fetching the supplier of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}
            

    @staticmethod
    async def warehouses(brand_id: str):
        """
        RETURNS THE WAREHOUSE OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select warehouse_id, name, phone, address, email
                            from warehouse where
                            brand_id = %s
                            '''
                    values = (brand_id, )
                    await cursor.execute(query, values)

                    warehouses= await cursor.fetchall()

                    warehouses = [{
                        "warehouse_id": warehouse.get("warehouse_id"),
                        "name": warehouse.get("name"),
                        "number": warehouse.get("phone"),
                        "email": warehouse.get("email"),
                        "address": json.loads(warehouse.get("address"))
                    } for warehouse in warehouses if warehouse]

                    return warehouses
            except Exception as e:
                print(f"error occured while fetching the warehouse of the brand for the brandid=>{brand_id}\n{e}")
                return {"error": e.args[0]}
            
    @staticmethod
    async def warehouse(brand_id: str, warehouse_id: str):
        """
        RETURNS THE WAREHOUSE OF THE BRAND
        """

        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor=DictCursor) as cursor:
                    query = '''
                            select name, phone, address, email
                            from warehouse where
                            brand_id = %s and warehouse_id = %s
                            '''
                    values = (brand_id, warehouse_id)
                    await cursor.execute(query, values)

                    warehouse = await cursor.fetchone()

                    warehouse = {
                        "warehouse_id": warehouse.get("warehouse_id"),
                        "name": warehouse.get("name"),
                        "number": warehouse.get("phone"),
                        "email": warehouse.get("email"),
                        "address": json.loads(warehouse.get("address"))
                    }

                    return warehouse
            except Exception as e:
                print(f"error occured while fetching the warehouse of the brand for the brandid=>{brand_id}\n{e}")
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