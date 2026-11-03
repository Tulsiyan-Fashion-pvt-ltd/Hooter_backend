CREATE TABLE flipkart_sellers (
    seller_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    brand_id VARCHAR(36) NOT NULL,
    refresh_token VARCHAR(256) NOT NULL,
    refresh_token_expires_in INT NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_flipkart_sellers_brand
        FOREIGN KEY (brand_id)
        REFERENCES brand(brand_id)
        ON DELETE CASCADE
);