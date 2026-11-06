ALTER TABLE master_inventory
    DROP PRIMARY KEY,
    ADD COLUMN `id` INT PRIMARY KEY AUTO_INCREMENT FIRST,
    ADD COLUMN `warehouse_id` INT DEFAULT NULL AFTER id,
    ADD COLUMN `status` ENUM('active', 'inactive') NOT NULL DEFAULT 'active',
    ADD FOREIGN KEY(`warehouse_id`) REFERENCES warehouse(`warehouse_id`);

DROP TABLE master_inventory_movements;

CREATE TABLE master_inventory_movements (
    movement_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    inventory_id INT NOT NULL,
    qty_change INT NOT NULL,
    reason ENUM('sale', 'return', 'inward', 'adjustment') NOT NULL,
    reference_id VARCHAR(128),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY(inventory_id) REFERENCES master_inventory(id) ON DELETE CASCADE
);