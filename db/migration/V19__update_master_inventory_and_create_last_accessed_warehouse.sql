ALTER TABLE master_inventory
MODIFY COLUMN warehouse_id INT NOT NULL;

CREATE TABLE last_accessed_warehouse (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    warehouse_id INT NOT NULL UNIQUE,
    brand_id VARCHAR(36) NOT NULL UNIQUE,

    CONSTRAINT FOREIGN KEY (warehouse_id) REFERENCES warehouse(warehouse_id) ON DELETE CASCADE,
    CONSTRAINT FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE
);