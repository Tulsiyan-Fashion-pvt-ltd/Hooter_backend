-- Altering the product images (image_variation) enum values to suit the server values

alter table product_images
modify column image_variation
enum('original', 'high_resol_webp', 'low_resol_webp', 'webp_card')
not null;



-- Creating seperate record to store image_url
-- to stop creating redundant data
CREATE TABLE image_urls (
    image_id INT,

    image_variation ENUM(
        'original',
        'high_resol_webp',
        'low_resol_webp',
        'webp_card'
    ) NOT NULL,

    image_url VARCHAR(125) NOT NULL UNIQUE,
    FOREIGN KEY(image_id) REFERENCES product_images(id)
    ON DELETE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;


-- altering the product_images and removing the data which is needing other records to be redundant
alter table product_images
drop column image_url,
drop index uq_usku_variation,
drop column image_variation,
add unique key(usku_id, image_type);
