from flask_mysqldb import MySQL

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('sql initialized')
    init_stores_tables(app)
    init_catalogue_tables(app)

class Write:
    @staticmethod
    def signup_user(user_creds):  #taking the arguments as objects or dict
        cursor = mysql.connection.cursor()
        try:
            userid = user_creds.get('userid')
            hashed_password = user_creds.get('hashed_password')
            name=user_creds.get('name')
            number = user_creds.get('number')
            email = user_creds.get('email')
            designation = user_creds.get('designation')


            cursor.execute('''insert into users(user_id, user_password)
                           values(%s, %s)
                           ''', (userid, hashed_password))

            cursor.execute('''insert into user_creds(user_id, user_name, phone_number, user_email, user_access, user_designation)
                           values(%s, %s, %s, %s, %s, %s)
                            ''', (userid, name, number, email, 'super_user', designation))

            mysql.connection.commit()
        except Exception as e:
            mysql.connection.rollback()
            cursor.close()
            print(f'error encounetered while signing up the user as {e}\n sql rollback')

            if (e.args[0] == 1062):
                return {'status': 'error', 'message': "user_already_registered"}

            return {'status': 'error', 'message': "unable_to_register_user"}
        finally:
            cursor.close()
        return {'status': 'ok', 'message': 'user_registeration_successfull'}

    @staticmethod
    def add_store(user_id: str, shopify_shop_name: str, shopify_access_token: str, store_name: str = None, is_primary: bool = False) -> dict:
        """Add a new Shopify store for a user."""
        from encryption import TokenEncryption
        
        cursor = mysql.connection.cursor()
        try:
            # Encrypt token before storage
            encrypted_token = TokenEncryption.encrypt_token(shopify_access_token)
            
            # If this is the first store, make it primary
            cursor.execute('SELECT COUNT(*) FROM stores WHERE user_id = %s', (user_id,))
            store_count = cursor.fetchone()[0]
            if store_count == 0:
                is_primary = True

            # If making this primary, unset other primary stores
            if is_primary:
                cursor.execute('''
                    UPDATE stores SET is_primary = FALSE 
                    WHERE user_id = %s AND is_primary = TRUE
                ''', (user_id,))

            cursor.execute('''
                INSERT INTO stores (user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active)
                VALUES (%s, %s, %s, %s, %s, TRUE)
            ''', (user_id, shopify_shop_name, encrypted_token, store_name or shopify_shop_name, is_primary))

            mysql.connection.commit()
            
            # Fetch and return the created store
            store_id = cursor.lastrowid
            store = Fetch.get_store_by_id(store_id, user_id)
            return {'status': 'ok', 'message': 'Store added successfully', 'store': store}

        except Exception as e:
            mysql.connection.rollback()
            cursor.close()
            print(f'Error adding store: {str(e)}')
            
            if hasattr(e, 'args') and e.args[0] == 1062:
                return {'status': 'error', 'message': 'Store already exists for this Shopify shop'}
            
            return {'status': 'error', 'message': f'Unable to add store: {str(e)}'}
        finally:
            cursor.close()

    @staticmethod
    def update_store(store_id: int, user_id: str, **kwargs) -> dict:
        """Update store details."""
        from encryption import TokenEncryption

        cursor = mysql.connection.cursor()
        try:
            # Verify ownership
            cursor.execute('SELECT user_id FROM stores WHERE store_id = %s', (store_id,))
            result = cursor.fetchone()
            if not result or result[0] != user_id:
                return {'status': 'error', 'message': 'Unauthorized access'}

            # Build update query
            updates = []
            params = []
            allowed_fields = ['shopify_shop_name', 'shopify_access_token', 'store_name', 'is_primary']
            
            for key, value in kwargs.items():
                if key in allowed_fields:
                    if key == 'shopify_access_token':
                        encrypted_token = TokenEncryption.encrypt_token(value)
                        updates.append('shopify_access_token_encrypted = %s')
                        params.append(encrypted_token)
                    else:
                        updates.append(f'{key} = %s')
                        params.append(value)

            if not updates:
                return {'status': 'error', 'message': 'No valid fields to update'}

            params.append(store_id)
            params.append(user_id)

            # If setting as primary, unset others
            if 'is_primary' in kwargs and kwargs['is_primary']:
                cursor.execute('''
                    UPDATE stores SET is_primary = FALSE 
                    WHERE user_id = %s AND is_primary = TRUE AND store_id != %s
                ''', (user_id, store_id))

            query = f"UPDATE stores SET {', '.join(updates)} WHERE store_id = %s AND user_id = %s"
            cursor.execute(query, params)
            mysql.connection.commit()

            store = Fetch.get_store_by_id(store_id, user_id)
            return {'status': 'ok', 'message': 'Store updated successfully', 'store': store}

        except Exception as e:
            mysql.connection.rollback()
            print(f'Error updating store: {str(e)}')
            return {'status': 'error', 'message': f'Unable to update store: {str(e)}'}
        finally:
            cursor.close()

    @staticmethod
    def delete_store(store_id: int, user_id: str) -> dict:
        """Delete a store (soft delete - mark as inactive)."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute('''
                UPDATE stores SET is_active = FALSE 
                WHERE store_id = %s AND user_id = %s
            ''', (store_id, user_id))
            
            affected_rows = cursor.rowcount
            mysql.connection.commit()

            if affected_rows == 0:
                return {'status': 'error', 'message': 'Store not found or unauthorized'}

            return {'status': 'ok', 'message': 'Store deleted successfully'}

        except Exception as e:
            mysql.connection.rollback()
            print(f'Error deleting store: {str(e)}')
            return {'status': 'error', 'message': f'Unable to delete store: {str(e)}'}
        finally:
            cursor.close()



class Fetch:
    @staticmethod
    def userid_by_email(email):
        cursor = mysql.connection.cursor()
        userid = None

        try:          
            cursor.execute('''
                           select user_id from user_creds where user_email=%s
                           ''', (email, ))

            userid = cursor.fetchone()
            userid = userid[0] if userid and len(userid) != 0 else None
        except Exception as e:
            print(f"error while checking the checking the credentials for login as {e}")
        finally:
            cursor.close()
        return userid
    
    @staticmethod
    def check_password(userid, hashed_password):
        cursor = mysql.connection.cursor()
        result = None
        try:
            cursor.execute('''select 1 from users where user_id=%s and user_password=%s
                           ''', (userid, hashed_password))
            
            result = cursor.fetchone()
            result = result[0]
        except Exception as e:
            print(f'error occured while checking the password as {e}')
        finally:
            cursor.close()
        return result
    
    @staticmethod
    def user_details(userid):
        user = userid
        cursor = mysql.connection.cursor()
        user_credentials = None # returned value
        try:
            if user == None:
                return ()
            else:
                cursor.execute('''select user_name, phone_number, user_email, user_designation, user_access
                               from user_creds
                               where user_id=%s
                               ''', (user, ))

                user_credentials = cursor.fetchone()
        except Exception as e:
            print(f'encountered error while fetching user credentials \n{e}')
        finally:
            cursor.close()

        return user_credentials

    @staticmethod
    def get_user_stores(user_id: str) -> list:
        """Fetch all stores for a user."""
        cursor = mysql.connection.cursor()
        stores = []
        try:
            cursor.execute('''
                SELECT store_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active
                FROM stores
                WHERE user_id = %s AND is_active = TRUE
                ORDER BY is_primary DESC, created_at DESC
            ''', (user_id,))
            
            results = cursor.fetchall()
            stores = [
                {
                    'store_id': row[0],
                    'shopify_shop_name': row[1],
                    'shopify_access_token_encrypted': row[2],
                    'store_name': row[3],
                    'is_primary': row[4],
                    'is_active': row[5]
                }
                for row in results
            ]
        except Exception as e:
            print(f'Error fetching user stores: {str(e)}')
        finally:
            cursor.close()
        return stores

    @staticmethod
    def get_store_by_id(store_id: int, user_id: str = None) -> dict:
        """Fetch a specific store. Optionally verify user ownership."""
        cursor = mysql.connection.cursor()
        store = None
        try:
            if user_id:
                cursor.execute('''
                    SELECT store_id, user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active
                    FROM stores
                    WHERE store_id = %s AND user_id = %s
                ''', (store_id, user_id))
            else:
                cursor.execute('''
                    SELECT store_id, user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary, is_active
                    FROM stores
                    WHERE store_id = %s
                ''', (store_id,))
            
            result = cursor.fetchone()
            if result:
                store = {
                    'store_id': result[0],
                    'user_id': result[1],
                    'shopify_shop_name': result[2],
                    'shopify_access_token_encrypted': result[3],
                    'store_name': result[4],
                    'is_primary': result[5],
                    'is_active': result[6]
                }
        except Exception as e:
            print(f'Error fetching store: {str(e)}')
        finally:
            cursor.close()
        return store

    @staticmethod
    def get_primary_store(user_id: str) -> dict:
        """Fetch the primary store for a user."""
        cursor = mysql.connection.cursor()
        store = None
        try:
            cursor.execute('''
                SELECT store_id, user_id, shopify_shop_name, shopify_access_token_encrypted, store_name, is_primary
                FROM stores
                WHERE user_id = %s AND is_primary = TRUE AND is_active = TRUE
                LIMIT 1
            ''', (user_id,))
            
            result = cursor.fetchone()
            if result:
                store = {
                    'store_id': result[0],
                    'user_id': result[1],
                    'shopify_shop_name': result[2],
                    'shopify_access_token_encrypted': result[3],
                    'store_name': result[4],
                    'is_primary': result[5]
                }
        except Exception as e:
            print(f'Error fetching primary store: {str(e)}')
        finally:
            cursor.close()
        return store


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
            )
        ''')

        mysql.connection.commit()
        print('Stores table initialized successfully')

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing stores table: {str(e)}')

    finally:
        if cursor:
            cursor.close()


def init_catalogue_tables(app):
    """Initialize catalogue and Shopify mapping tables with multi-client support."""
    cursor = None
    try:
        cursor = mysql.connection.cursor()

        # Create catalogue table with store_id for multi-client support
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue (
                catalogue_id VARCHAR(36) PRIMARY KEY,
                store_id INT NOT NULL,
                title VARCHAR(255) NOT NULL,
                description LONGTEXT NOT NULL,
                vendor VARCHAR(100),
                product_type VARCHAR(100),
                tags VARCHAR(500),
                status ENUM('DRAFT', 'ACTIVE', 'ARCHIVED') DEFAULT 'ACTIVE',
                price DECIMAL(10, 2) NOT NULL,
                user_id VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_store_id (store_id),
                INDEX idx_user_id (user_id),
                INDEX idx_created_at (created_at),
                INDEX idx_status (status),
                FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE CASCADE
            )
        ''')

        # Create Shopify mapping table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_shopify_mapping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                catalogue_id VARCHAR(36) NOT NULL UNIQUE,
                shopify_product_id VARCHAR(255) NOT NULL,
                store_id INT NOT NULL,
                last_sync_status ENUM('SUCCESS', 'FAILED') DEFAULT 'SUCCESS',
                sync_error_message LONGTEXT,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_catalogue_id (catalogue_id),
                INDEX idx_shopify_product_id (shopify_product_id),
                INDEX idx_store_id (store_id),
                FOREIGN KEY (catalogue_id) REFERENCES catalogue(catalogue_id) ON DELETE CASCADE,
                FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE
            )
        ''')

        # Create variants table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_variants (
                variant_id VARCHAR(36) PRIMARY KEY,
                catalogue_id VARCHAR(36) NOT NULL,
                shopify_variant_id VARCHAR(255) NOT NULL,
                sku VARCHAR(100),
                price DECIMAL(10, 2) NOT NULL,
                compare_at_price DECIMAL(10, 2),
                weight DECIMAL(8, 3),
                weight_unit VARCHAR(10),
                barcode VARCHAR(100),
                title VARCHAR(255),
                position INT DEFAULT 0,
                inventory_item_id VARCHAR(255),
                status ENUM('ACTIVE', 'ARCHIVED') DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_catalogue_id (catalogue_id),
                INDEX idx_shopify_variant_id (shopify_variant_id),
                INDEX idx_sku (sku),
                FOREIGN KEY (catalogue_id) REFERENCES catalogue(catalogue_id) ON DELETE CASCADE
            )
        ''')

        # Create images table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_images (
                image_id VARCHAR(36) PRIMARY KEY,
                catalogue_id VARCHAR(36) NOT NULL,
                shopify_media_id VARCHAR(255),
                image_url LONGTEXT NOT NULL,
                image_local_path VARCHAR(500),
                alt_text VARCHAR(255),
                position INT DEFAULT 0,
                uploaded_by VARCHAR(100),
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_catalogue_id (catalogue_id),
                INDEX idx_position (position),
                FOREIGN KEY (catalogue_id) REFERENCES catalogue(catalogue_id) ON DELETE CASCADE,
                FOREIGN KEY (uploaded_by) REFERENCES users(user_id) ON DELETE SET NULL
            )
        ''')

        # Create inventory table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_inventory (
                inventory_id INT AUTO_INCREMENT PRIMARY KEY,
                variant_id VARCHAR(36) NOT NULL,
                location_id VARCHAR(255) NOT NULL,
                available_quantity INT DEFAULT 0,
                reserved_quantity INT DEFAULT 0,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sync_status ENUM('IN_SYNC', 'OUT_OF_SYNC') DEFAULT 'IN_SYNC',
                UNIQUE KEY unique_variant_location (variant_id, location_id),
                INDEX idx_variant_id (variant_id),
                FOREIGN KEY (variant_id) REFERENCES catalogue_variants(variant_id) ON DELETE CASCADE
            )
        ''')

        # Create audit log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_audit_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                catalogue_id VARCHAR(36),
                store_id INT,
                user_id VARCHAR(100),
                action ENUM('CREATE', 'UPDATE', 'DELETE', 'SYNC', 'INVENTORY') NOT NULL,
                changes JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_catalogue_id (catalogue_id),
                INDEX idx_user_id (user_id),
                FOREIGN KEY (catalogue_id) REFERENCES catalogue(catalogue_id) ON DELETE SET NULL,
                FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE SET NULL,
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE SET NULL
            )
        ''')

        # Create idempotency table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_idempotency (
                id INT AUTO_INCREMENT PRIMARY KEY,
                idempotency_key VARCHAR(128) NOT NULL,
                user_id VARCHAR(100) NOT NULL,
                store_id INT NOT NULL,
                response_json LONGTEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uniq_idem_user_store (idempotency_key, user_id, store_id),
                INDEX idx_idempotency_key (idempotency_key),
                FOREIGN KEY (store_id) REFERENCES stores(store_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES user_creds(user_id) ON DELETE CASCADE
            )
        ''')

        mysql.connection.commit()
        print('Catalogue tables initialized successfully')

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing catalogue tables: {str(e)}')

    finally:
        if cursor:
            cursor.close()