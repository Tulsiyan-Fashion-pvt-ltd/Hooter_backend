from inventory.warehouse import mariadb
import json
import indiapins
from . import model
from .authorize import warehouse_access_required

async def delete_warehouse(warehouse_id: str) -> tuple[dict[str, str], int]:
    """Deletes the warehouse from rdbms(mariadb)
    
    Args:
        warehouse_id:

    Returns:
        returns the `tuple()` with 
        - dict:
        - api status

    """
    db_response = await mariadb.Write.delete_warehouse(warehouse_id)
    
    if db_response != "ok":
        return {"status": "request failed", "message": "Error occured while deleting the warehouse"}, 500

    return {"status": "successful", "message": "Warehouse deleted successfully"}, 200



async def update_warehouse(warehouse_id: str, data: dict) -> tuple[dict[str, str], int]:
    """Updates the warehouse in MariaDB

    Args:
        warehouse_id:
             unique id for warehouse
        data - attribute and value for warehouse as key:value pair

    Returns:
      tuple(0) = dict and tuple(1) = rest status
      """
    print(data)
    db_response = await mariadb.Write.update_warehouse(warehouse_id, data)

    if db_response != "ok":
        if db_response.get("error") == 1062:
            return {"status": "failed", "message": "Duplicate data"}, 409
        else:
            return {"status": "request failed", "message": "Error occured while updating the warehouse"}, 500

    return {"status": "successful", "message": "Warehouse updated successfully"}, 200



async def upload_warehouse(brand_id: str, warehouse_data: dict) -> tuple[dict[str, str], int]:
    """Uploads the given product to the database
    
    Args:
        - brand_id - Unique ID for the brand
        - warehouse_data: dict object for warehouse containing the name, number email and address data.

    Returns:
        - tuple(0) = dict and tuple(1) = rest status code
        """
    
    warehouse_id = await mariadb.Write.warehouse(brand_id, warehouse_data)
    if warehouse_id == "error": 
        return {"status": "failed", "message": "failed to add the warehouse"}, 500
    return {"status": "successful", "message": "added the warehouse", "warehouse_id": warehouse_id}, 200



async def get_warehouses(brand_id: str) -> tuple[dict[str, str|int], int]:
    """Fetch warehouses of the brand 
    
    Args:
        brand_id:
            Unique ID for the brand
            
    Returns:
        tuple(0) -> dict:
            - on successful -> warehouse data
            - on failure -> status and message
            
        tuple(1)-> int:
            rest api status code
    """
    warehouses = await mariadb.Fetch.warehouses(brand_id)
    warehouses = [{
        "warehouse_id": warehouse.get("warehouse_id"),
        "alias": warehouse.get('alias'),
        "name": warehouse.get("name"),
        "number": warehouse.get("phone"),
        "email": warehouse.get("email"),
        'status': warehouse.get('status'),
        "address": {
            "address": warehouse.get('address'),
            'city': warehouse.get('city'),
            'state': warehouse.get('state'),
            'pincode': warehouse.get('pincode')
        }
    } for warehouse in warehouses if warehouse]
    if warehouses == "error":
        return {"status": "failed", "message": "Failed to fetch warehouses" }, 500

    return warehouses, 200



@warehouse_access_required
async def get_warehouse(warehouse_id: str) -> tuple[dict[str, str|int], int]:
    """Fetch warehouse of the brand 
    
    Args:
        warehosue_id:
            Unique ID for the warehouse
            
    Returns:
        tuple(0) -> dict:
            - on successful -> warehouse data
            - on failure -> status and message
            
        tuple(1)-> int:
            rest api status code
    """
    warehouse = await mariadb.Fetch.warehouse(warehouse_id)
    warehouse = {
        "warehouse_id": warehouse.get("warehouse_id"),
        'alias': warehouse.get('alias'),
        "name": warehouse.get("name"),
        "phone_number": warehouse.get("phone_number"),
        "email": warehouse.get("email"),
        "address":  {
            "address": warehouse.get('address'),
            'city': warehouse.get('city'),
            'state': warehouse.get('state'),
            'pincode': warehouse.get('pincode')
        }
    } 


    if warehouse == "error":
        return {"status": "failed", "message": "Failed to fetch warehouses" }, 500

    return warehouse, 200
