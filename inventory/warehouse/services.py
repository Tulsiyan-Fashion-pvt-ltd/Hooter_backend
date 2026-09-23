from inventory.warehouse import mariadb

async def delete_warehouse(warehouse_id: str) -> tuple[dict[str, str], int]:
    """Deletes the warehouse from rdbms(mariadb)
    
    Parameters:
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
    """Updates the warehouse in MariaDB.
    Args:
      - warehouse_id - unique id for warehouse
      - data - attribute and value for warehouse as key:value pair

    Returns:
      tuple(0) = dict and tuple(1) = rest status
      """

    db_response = await mariadb.Write.update_warehouse(warehouse_id, data)

    if db_response != "ok":
        if db_response.get("error") == 1062:
            return {"status": "failed", "message": "Duplicate data"}, 409
        else:
            return {"status": "request failed", "message": "Error occured while updating the warehouse"}, 500

    return {"status": "successful", "message": "Warehouse updated successfully"}, 200