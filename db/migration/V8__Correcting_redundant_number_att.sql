ALTER TABLE warehouse
    DROP COLUMN phone,
    RENAME COLUMN number TO phone_number;

ALTER TABLE supplier
    CHANGE COLUMN number phone_number char(10) not null; 