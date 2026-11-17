DROP TABLE IF EXISTS master_inventory_movements;
DROP TABLE IF EXISTS master_inventory;

CREATE TABLE master_inventory (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    warehouse_id INT DEFAULT NULL,
    usku_id VARCHAR(64) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    status ENUM('active', 'inactive') NOT NULL DEFAULT 'active',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY unique_warehouse_usku (warehouse_id, usku_id),

    CONSTRAINT FOREIGN KEY (warehouse_id) REFERENCES warehouse(warehouse_id),
    CONSTRAINT FOREIGN KEY (usku_id) REFERENCES usku_record(usku_id) ON DELETE CASCADE
);

CREATE TABLE master_inventory_movements (
    movement_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    inventory_id INT NOT NULL,
    qty_change INT NOT NULL,
    movement_type ENUM('added', 'removed') NOT NULL,
    reason ENUM('sale', 'return', 'inwarding', 'adjustment') NOT NULL,
    reference_id VARCHAR(128) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT FOREIGN KEY (inventory_id) REFERENCES master_inventory(id) ON DELETE CASCADE
);