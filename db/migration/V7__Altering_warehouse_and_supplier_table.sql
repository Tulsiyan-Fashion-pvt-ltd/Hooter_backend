ALTER TABLE warehouse
    -- Remove old constraints
    DROP INDEX phone,
    DROP INDEX email,

    -- Modify existing columns
    MODIFY COLUMN name VARCHAR(36) NOT NULL,
    MODIFY COLUMN address TEXT NULL,
    MODIFY COLUMN email VARCHAR(256) NOT NULL,

    -- Add new columns
    ADD COLUMN alias VARCHAR(16) NOT NULL AFTER brand_id,
    ADD COLUMN number CHAR(10) NOT NULL AFTER name,
    ADD COLUMN city VARCHAR(128) NOT NULL AFTER address,
    ADD COLUMN state VARCHAR(128) NOT NULL AFTER city,
    ADD COLUMN pincode CHAR(6) NOT NULL AFTER state,

    -- Add brand scoped unique constraints
    ADD CONSTRAINT uq_warehouse_alias UNIQUE (brand_id, alias),
    ADD CONSTRAINT uq_warehouse_name UNIQUE (brand_id, name),
    ADD CONSTRAINT uq_warehouse_number UNIQUE (brand_id, number),
    ADD CONSTRAINT uq_warehouse_email UNIQUE (brand_id, email);



ALTER TABLE supplier
    -- Remove old global unique constraints
    DROP INDEX contact_number,
    DROP INDEX email,

    -- Modify existing columns
    CHANGE COLUMN contact_number number VARCHAR(12) NOT NULL,
    MODIFY COLUMN address TEXT NULL,
    MODIFY COLUMN email VARCHAR(128) NULL,

    -- Add new columns
    ADD COLUMN alias VARCHAR(16) NOT NULL AFTER brand_id,
    ADD COLUMN city VARCHAR(128) NOT NULL AFTER address,
    ADD COLUMN state VARCHAR(128) NOT NULL AFTER city,
    ADD COLUMN pincode CHAR(6) NOT NULL AFTER state,

    -- Add brand-scoped unique constraints
    ADD CONSTRAINT uq_supplier_alias UNIQUE (brand_id, alias),
    ADD CONSTRAINT uq_supplier_name UNIQUE (brand_id, name),
    ADD CONSTRAINT uq_supplier_number UNIQUE (brand_id, number),
    ADD CONSTRAINT uq_supplier_email UNIQUE (brand_id, email);