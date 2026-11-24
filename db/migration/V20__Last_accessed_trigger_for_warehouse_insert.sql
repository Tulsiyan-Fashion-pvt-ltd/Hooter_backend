CREATE TRIGGER update_last_accessed_warehouse_on_new_warehouse_upload
AFTER INSERT ON warehouse
FOR EACH ROW
BEGIN
    IF NOT EXISTS(    
            SELECT warehouse_id 
            FROM last_accessed_warehouse
            WHERE brand_id = NEW.brand_id
        )
    THEN
        INSERT INTO last_accessed_warehouse(warehouse_id, brand_id)
        VALUES (NEW.warehouse_id, NEW.brand_id);
    ELSE
        UPDATE last_accessed_warehouse
        SET warehouse_id=NEW.warehouse_id
        WHERE brand_id = NEW.brand_id;
    END IF;
END;