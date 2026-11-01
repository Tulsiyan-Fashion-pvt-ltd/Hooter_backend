CREATE TABLE brand_poc(
    `id` INT PRIMARY KEY AUTO_INCREMENT,
    `brand_id` VARCHAR(36) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `number` VARCHAR(255) UNIQUE NOT NULL,
    `email` VARCHAR(512) UNIQUE NOT NULL,
    `title` VARCHAR(100),
    FOREIGN KEY(brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE
);


ALTER TABLE brand 
    DROP key poc,
    DROP CONSTRAINT brand_ibfk_1,
    DROP COLUMN poc,
    RENAME COLUMN gstin TO gtin;