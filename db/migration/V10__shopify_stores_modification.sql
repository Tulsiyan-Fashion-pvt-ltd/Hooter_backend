ALTER TABLE `shopify_stores`
    DROP COLUMN shopify_access_token_encrypted,
    ADD COLUMN `encrypted_shopify_refresh_token` VARCHAR(512) NOT NULL
        AFTER `shopify_shop_name`,
    ADD COLUMN `refresh_token_expires_in_seconds` INT NOT NULL DEFAULT 7776000
        AFTER `encrypted_shopify_refresh_token`,
    MODIFY COLUMN created_at TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    MODIFY COLUMN updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp()
        ON UPDATE current_timestamp(),
    add column status enum('active', 'inactive') not null default 'active' 
        after "refresh_token_expires_in_seconds";