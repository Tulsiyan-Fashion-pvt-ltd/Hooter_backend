# this file initialises the mysql db gives mysql object which other files can import and take use of

from flask_mysqldb import MySQL
from quart import current_app
from pymongo import MongoClient
from quart_motor import Motor
import asyncmy
import os
from dotenv import load_dotenv

load_dotenv() # to load the .env file

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('initialized the sql')


#initializing mongodb
# using current_app.mongo to get the mongo object
# mongo = current_app.mongo
# await mongo.db.collection.operation()


def __init_mongodb__(app):
    app.config['MONGO_URI'] = os.environ.get('MONGO_HOST')
    app.mongo = Motor(app)

def init_stores_tables(app):
    """Initialize stores and client-store mapping tables for multi-client support."""
    cursor = None
    try:
        cursor = mysql.connection.cursor()

        # Create stores table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stores (
                store_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(100) NOT NULL,
                shopify_shop_name VARCHAR(255) NOT NULL,
                shopify_access_token_encrypted LONGTEXT NOT NULL,
                store_name VARCHAR(255),
                is_primary BOOLEAN DEFAULT FALSE,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_user_primary (user_id, is_primary),
                INDEX idx_user_id (user_id),
                INDEX idx_shop_name (shopify_shop_name),
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        mysql.connection.commit()

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing stores table: {str(e)}')

    finally:
        if cursor:
            cursor.close()


def init_users_tables(app):
    """Initialize users and user credentials tables for authentication and multi-user support."""
    cursor = None
    try:
        cursor = mysql.connection.cursor()

        # Create users table - core authentication data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(100) PRIMARY KEY,
                user_password LONGTEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB
        ''')

        # Create user_creds table - user profile and metadata
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_creds (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(100) NOT NULL UNIQUE,
                user_name VARCHAR(255),
                phone_number VARCHAR(20),
                user_email VARCHAR(255) NOT NULL UNIQUE,
                user_access VARCHAR(50) DEFAULT 'user',
                user_designation VARCHAR(100),
                created_at DATE DEFAULT (CURDATE()),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_user_email (user_email),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        mysql.connection.commit()

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing users tables: {str(e)}')

    finally:
        if cursor:
            cursor.close()


def init_brand_tables(app):
    """Initialize brand management tables for brand-centric architecture."""
    cursor = None
    try:
        cursor = mysql.connection.cursor()

        # Create brand table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS brand (
                brand_id INT AUTO_INCREMENT PRIMARY KEY,
                brand_name VARCHAR(255) NOT NULL,
                brand_logo VARCHAR(500),
                brand_description LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_brand_name (brand_name)
            ) ENGINE=InnoDB
        ''')

        # Create brand access table for user-brand mapping
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS brand_access (
                id INT AUTO_INCREMENT PRIMARY KEY,
                brand_id INT NOT NULL,
                user_id VARCHAR(100) NOT NULL,
                permission_level VARCHAR(50) DEFAULT 'editor',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_brand_user (brand_id, user_id),
                INDEX idx_brand_id (brand_id),
                INDEX idx_user_id (user_id),
                FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        mysql.connection.commit()

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing brand tables: {str(e)}')

    finally:
        if cursor:
            cursor.close()


def init_fashion_tables(app):
    """Initialize fashion product and Shopify mapping tables based on manager-approved schema."""
    cursor = None
    try:
        cursor = mysql.connection.cursor()

        # Create uid_record table - maintains unique product identifiers and brand association
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS uid_record (
                uid VARCHAR(36) PRIMARY KEY,
                brand_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_uid (uid),
                INDEX idx_brand_id (brand_id),
                INDEX idx_brand_uid (brand_id, uid),
                FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        # Create fashion table - main product table (replaces catalogue)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fashion (
                uid VARCHAR(36) PRIMARY KEY,
                brand_id INT NOT NULL,
                title VARCHAR(255) NOT NULL,
                description LONGTEXT,
                vendor VARCHAR(100),
                product_type VARCHAR(100),
                tags VARCHAR(500),
                status ENUM('DRAFT', 'ACTIVE', 'ARCHIVED') DEFAULT 'ACTIVE',
                price DECIMAL(10, 2),
                compare_at_price DECIMAL(10, 2),
                sku VARCHAR(100),
                barcode VARCHAR(100),
                weight DECIMAL(8, 3),
                weight_unit VARCHAR(10),
                collections VARCHAR(500),
                brand_color VARCHAR(50),
                product_remark LONGTEXT,
                series_length_ankle VARCHAR(50),
                series_rise_waist VARCHAR(50),
                series_knee VARCHAR(50),
                gender VARCHAR(50),
                fit_type VARCHAR(100),
                print_type VARCHAR(100),
                material VARCHAR(255),
                material_composition VARCHAR(500),
                care_instruction LONGTEXT,
                art_technique VARCHAR(100),
                stitch_type VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_brand_id (brand_id),
                INDEX idx_status (status),
                INDEX idx_created_at (created_at),
                INDEX idx_sku (sku),
                FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE,
                FOREIGN KEY (uid) REFERENCES uid_record(uid) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        # Create low_resol_images table (replaces catalogue_images)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS low_resol_images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                uid VARCHAR(36) NOT NULL,
                image_url LONGTEXT NOT NULL,
                position INT DEFAULT 0,
                alt_text VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_uid (uid),
                INDEX idx_position (position),
                FOREIGN KEY (uid) REFERENCES uid_record(uid) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        # Create hsn_record table - HSN/tax classification
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hsn_record (
                hsn VARCHAR(10) PRIMARY KEY,
                uid VARCHAR(36) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_uid (uid),
                FOREIGN KEY (uid) REFERENCES uid_record(uid) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        # Create product_info_change_stack table (replaces catalogue_audit_log)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS product_info_change_stack (
                id INT AUTO_INCREMENT PRIMARY KEY,
                uid VARCHAR(36) NOT NULL,
                brand_id INT NOT NULL,
                user_id VARCHAR(100),
                action ENUM('CREATE', 'UPDATE', 'DELETE', 'SYNC', 'INVENTORY') NOT NULL,
                changed_attribute JSON,
                update_date DATE DEFAULT (CURRENT_DATE),
                update_time TIME DEFAULT (CURRENT_TIME),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_uid (uid),
                INDEX idx_brand_id (brand_id),
                INDEX idx_user_id (user_id),
                INDEX idx_action (action),
                FOREIGN KEY (uid) REFERENCES uid_record(uid) ON DELETE CASCADE,
                FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE SET NULL
            ) ENGINE=InnoDB
        ''')

        # Create shopify_product_mapping table (replaces catalogue_shopify_mapping)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shopify_product_mapping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                uid VARCHAR(36) NOT NULL,
                brand_id INT NOT NULL,
                shopify_product_id VARCHAR(255) NOT NULL,
                store_id INT NOT NULL,
                last_sync_status ENUM('SUCCESS', 'FAILED') DEFAULT 'SUCCESS',
                sync_error_message LONGTEXT,
                synced_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_uid_store (uid, store_id),
                INDEX idx_uid (uid),
                INDEX idx_brand_id (brand_id),
                INDEX idx_shopify_product_id (shopify_product_id),
                INDEX idx_store_id (store_id),
                FOREIGN KEY (uid) REFERENCES uid_record(uid) ON DELETE CASCADE,
                FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE,
                FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        # Keep idempotency table for duplicate prevention
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_idempotency (
                id INT AUTO_INCREMENT PRIMARY KEY,
                idempotency_key VARCHAR(128) NOT NULL,
                user_id VARCHAR(100) NOT NULL,
                brand_id INT NOT NULL,
                response_json LONGTEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_idem_user_brand (idempotency_key, user_id, brand_id),
                INDEX idx_idempotency_key (idempotency_key),
                INDEX idx_brand_id (brand_id),
                FOREIGN KEY (brand_id) REFERENCES brand(brand_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE CASCADE
            ) ENGINE=InnoDB
        ''')

        mysql.connection.commit()

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing fashion tables: {str(e)}')

    finally:
        if cursor:
            cursor.close()