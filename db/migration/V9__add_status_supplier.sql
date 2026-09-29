ALTER TABLE supplier
    ADD COLUMN status enum('active', 'inactive') default 'active' not null;