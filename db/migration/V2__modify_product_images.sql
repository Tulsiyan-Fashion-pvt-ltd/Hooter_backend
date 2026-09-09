drop table product_images;

CREATE TABLE product_images (
    usku_id VARCHAR(64) NOT NULL,
    image_url VARCHAR(255) NOT NULL,
    image_type VARCHAR(32) NOT NULL,
    image_order VARCHAR(2) NOT NULL,
    id INT NOT NULL AUTO_INCREMENT,
    image_variation ENUM('original','high_resol','low_resol','webp_card') NOT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_usku_variation (usku_id, image_type, image_variation),
    UNIQUE KEY uq_image_url (image_url),

    CONSTRAINT product_images_ibfk_1
        FOREIGN KEY (usku_id)
        REFERENCES usku_record (usku_id)
        ON DELETE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;