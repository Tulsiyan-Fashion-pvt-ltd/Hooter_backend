-- Current stock for each SKU
CREATE TABLE master_inventory (
    usku_id     VARCHAR(64) PRIMARY KEY,
    stock       INT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP NOT NULL
                DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (usku_id)
    REFERENCES usku_record(usku_id)
    ON DELETE CASCADE
);

-- Audit trail of every inventory change
CREATE TABLE master_inventory_movements (
    movement_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    usku_id       VARCHAR(64) NOT NULL,
    qty_change    INT NOT NULL,
    reason        ENUM('sale', 'return', 'inward', 'adjustment') NOT NULL,
    reference_id  VARCHAR(128),
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_inventory_movement_usku
        FOREIGN KEY (usku_id)
        REFERENCES master_inventory(usku_id)
        ON DELETE CASCADE,

    INDEX idx_usku_created (usku_id, created_at),
    INDEX idx_reference (reference_id)
);


ALTER TABLE usku_record
    DROP COLUMN stock
;


CREATE TRIGGER trg_create_inventory
AFTER UPDATE ON usku_record
FOR EACH ROW
BEGIN
    IF NEW.status = 'completed'
       AND OLD.status <> 'completed' THEN

        INSERT INTO master_inventory (usku_id)
        VALUES (NEW.usku_id);

    END IF;
END;