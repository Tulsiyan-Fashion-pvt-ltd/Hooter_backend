from quart import current_app, json
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
                                    rejected, uom)
                                    values(%s, %s, %s, %s, %s, %s, %s)
                                '''
                        values = (inward_id, usku_id, obj.get("po"), obj.get("exp_stock"), obj.get("receievd", 0), 
                                  obj.get("rejected", 0), obj.get("uom"))
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
            except Exception as e:
                print(f"error occured while creating inward\n{e}")
                await connection.rollback()
                return "error"

        
            
class Update:
    async def inward(inward_data: dict, brand_id: str) -> str:
        pool = current_app.pool
        async with pool.acquire() as connection:
            try:
                async with connection.cursor(cursor = DictCursor) as cursor:
                    status = inward_data.get("status")
                    inward_id = inward_data.get("inward_id")
                    if not status:
                        raise Exception("inward status is missing") 
                    
                    ''' if inward is completed do not update it '''
                    query = '''select inward_status from inward where inward_id = %s'''
                    values = (inward_id, )

                    await cursor.execute(query, values)
                    prev_status = await cursor.fetchone()

                    if prev_status.get("inward_status") in ["completed", "cancelled"]:
                        return "not allowed"
                    
                    query = '''
                            update inward set
                            inward_status = %s,
                            updated_at = %s
                            where inward_id = %s and brand_id = %s
                            '''
                    values = (status, datetime.now(), inward_data.get("inward_id"), brand_id)
                    await cursor.execute(query, values)
                    for unit in inward_data.get("usku_ids", []):
                        query = '''
                                update inward_items set
                                received_qtt = received_qtt + %s,
                                rejected= rejected + %s where
                                inward_id = %s and usku_id = %s 
                                '''
                        values = (unit.get("receievd", 0), unit.get("rejected", 0), 
                                  inward_data.get("inward_id"), unit.get("usku_id"))
                        
                        await cursor.execute(query, values)

                        query = '''
                            update catalog
                            join usku_record as u on catalog.usku_id = u.usku_id
                            set catalog.product_stock = catalog.product_stock + %s
                            where
                            catalog.usku_id = %s and u.brand_id = %s
                        '''
                        values = ((unit.get("receievd", 0) - unit.get("rejected", 0)), unit.get("usku_id"), brand_id)

                        await cursor.execute(query, values)

                    '''create the grn record'''
                    prefix = "GRN"
                    year = datetime.now().date().year
                    count = 0 
                    query = '''
                            select count(grn_id) as count from grn where inward_id = %s
                            '''
                    values = (inward_id, )
                    await cursor.execute(query, values)
                    count = await cursor.fetchone()

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
                print(f"Error countered while updating the inward for brand {brand_id}\n {e}")
                return "error"
            

class Fetch:
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
                        query = '''select created_at, inward_status from inward where inward_id = %s and brand_id = %s'''
                        values = (inward_id, brand_id)

                        await cursor.execute(query, values)
                        inward = await cursor.fetchone()
                        
                        # if inward.get("inward_status") == "completed":
                        #     return "not allowed"

                        query = '''select img.image_type, img.image_url, u.usku_id, u.sku_id, c.product_title, niche.product_name as product_type, 
                                inward_items.uom, 
                                case when inward.inward_status = "partial" then 
                                (inward_items.expected_qtt - inward_items.received_qtt) 
                                else inward_items.expected_qtt end as expected_qtt
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

                        if not inward or not inward_items:
                            return "error"
                        else:
                            return {"created_at": inward.get("created_at"), "uskus": inward_items, "status": inward.get("inward_status")}

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
                                select inward.inward_id, inward.inward_status, inward.created_at, inward.updated_at, s.name as supplier
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