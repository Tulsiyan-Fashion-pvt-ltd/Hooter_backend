from flask_mysqldb import MySQL

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    # SQL initialized
    init_brand_tables(app)
    init_stores_tables(app)
    init_fashion_tables(app)

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

            cursor.execute('''insert into user_creds(user_id, user_name, phone_number, user_email, user_access, user_designation, created_at)
                           values(%s, %s, %s, %s, %s, %s, CURDATE())
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

    @staticmethod
    def create_brand(brand_name: str, user_id: str, brand_logo: str = None, brand_description: str = None) -> dict:
        """Create a new brand and assign it to the user."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute('''
                INSERT INTO brand (brand_name, brand_logo, brand_description)
                VALUES (%s, %s, %s)
            ''', (brand_name, brand_logo, brand_description))

            brand_id = cursor.lastrowid

            cursor.execute('''
                INSERT INTO brand_access (brand_id, user_id, permission_level)
                VALUES (%s, %s, %s)
            ''', (brand_id, user_id, 'owner'))

            mysql.connection.commit()
            
            brand = Fetch.get_brand_by_id(brand_id)
            return {'status': 'ok', 'message': 'Brand created successfully', 'brand': brand}

        except Exception as e:
            mysql.connection.rollback()
            cursor.close()
            print(f'Error creating brand: {str(e)}')
            
            if hasattr(e, 'args') and e.args[0] == 1062:
                return {'status': 'error', 'message': 'Brand already exists'}
            
            return {'status': 'error', 'message': f'Unable to create brand: {str(e)}'}
        finally:
            cursor.close()

    @staticmethod
    def create_product(uid: str, brand_id: int, title: str, description: str, **kwargs) -> dict:
        """Create a new product (fashion entry) with extended attributes."""
        cursor = mysql.connection.cursor()
        try:
            # Create uid record first (associate with brand)
            cursor.execute('INSERT INTO uid_record (uid, brand_id) VALUES (%s, %s)', (uid, brand_id))

            # Insert fashion product
            cursor.execute('''
                INSERT INTO fashion (
                    uid, brand_id, title, description, vendor, product_type, tags,
                    status, price, compare_at_price, sku, barcode, weight, weight_unit,
                    collections, brand_color, product_remark, series_length_ankle,
                    series_rise_waist, series_knee, gender, fit_type, print_type,
                    material, material_composition, care_instruction, art_technique, stitch_type
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            ''', (
                uid, brand_id, title, description,
                kwargs.get('vendor'), kwargs.get('product_type'), kwargs.get('tags'),
                kwargs.get('status', 'ACTIVE'),
                kwargs.get('price'), kwargs.get('compare_at_price'),
                kwargs.get('sku'), kwargs.get('barcode'),
                kwargs.get('weight'), kwargs.get('weight_unit'),
                kwargs.get('collections'), kwargs.get('brand_color'),
                kwargs.get('product_remark'), kwargs.get('series_length_ankle'),
                kwargs.get('series_rise_waist'), kwargs.get('series_knee'),
                kwargs.get('gender'), kwargs.get('fit_type'), kwargs.get('print_type'),
                kwargs.get('material'), kwargs.get('material_composition'),
                kwargs.get('care_instruction'), kwargs.get('art_technique'), kwargs.get('stitch_type')
            ))

            mysql.connection.commit()
            return {'status': 'ok', 'message': 'Product created successfully', 'uid': uid}

        except Exception as e:
            mysql.connection.rollback()
            print(f'Error creating product: {str(e)}')
            
            if hasattr(e, 'args') and e.args[0] == 1062:
                return {'status': 'error', 'message': 'Product UID already exists'}
            
            return {'status': 'error', 'message': f'Unable to create product: {str(e)}'}
        finally:
            cursor.close()

    @staticmethod
    def update_product(uid: str, brand_id: int, **kwargs) -> dict:
        """Update product details."""
        cursor = mysql.connection.cursor()
        try:
            updates = []
            params = []
            allowed_fields = [
                'title', 'description', 'vendor', 'product_type', 'tags', 'status',
                'price', 'compare_at_price', 'sku', 'barcode', 'weight', 'weight_unit',
                'collections', 'brand_color', 'product_remark', 'series_length_ankle',
                'series_rise_waist', 'series_knee', 'gender', 'fit_type', 'print_type',
                'material', 'material_composition', 'care_instruction', 'art_technique', 'stitch_type'
            ]

            for key, value in kwargs.items():
                if key in allowed_fields:
                    updates.append(f'{key} = %s')
                    params.append(value)

            if not updates:
                return {'status': 'error', 'message': 'No valid fields to update'}

            params.extend([uid, brand_id])
            query = f"UPDATE fashion SET {', '.join(updates)} WHERE uid = %s AND brand_id = %s"
            cursor.execute(query, params)
            mysql.connection.commit()

            return {'status': 'ok', 'message': 'Product updated successfully', 'uid': uid}

        except Exception as e:
            mysql.connection.rollback()
            print(f'Error updating product: {str(e)}')
            return {'status': 'error', 'message': f'Unable to update product: {str(e)}'}
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

    @staticmethod
    def get_user_brands(user_id: str) -> list:
        """Fetch all brands assigned to a user."""
        cursor = mysql.connection.cursor()
        brands = []
        try:
            cursor.execute('''
                SELECT b.brand_id, b.brand_name, b.brand_logo, b.brand_description, b.created_at
                FROM brand b
                INNER JOIN brand_access ba ON b.brand_id = ba.brand_id
                WHERE ba.user_id = %s
                ORDER BY b.created_at DESC
            ''', (user_id,))

            results = cursor.fetchall()
            brands = [
                {
                    'brand_id': row[0],
                    'brand_name': row[1],
                    'brand_logo': row[2],
                    'brand_description': row[3],
                    'created_at': row[4]
                }
                for row in results
            ]
        except Exception as e:
            print(f'Error fetching user brands: {str(e)}')
        finally:
            cursor.close()
        return brands

    @staticmethod
    def get_brand_by_id(brand_id: int) -> dict:
        """Fetch a specific brand by ID."""
        cursor = mysql.connection.cursor()
        brand = None
        try:
            cursor.execute('''
                SELECT brand_id, brand_name, brand_logo, brand_description, created_at
                FROM brand
                WHERE brand_id = %s
            ''', (brand_id,))

            result = cursor.fetchone()
            if result:
                brand = {
                    'brand_id': result[0],
                    'brand_name': result[1],
                    'brand_logo': result[2],
                    'brand_description': result[3],
                    'created_at': result[4]
                }
        except Exception as e:
            print(f'Error fetching brand: {str(e)}')
        finally:
            cursor.close()
        return brand

    @staticmethod
    def verify_brand_ownership(brand_id: int, user_id: str) -> bool:
        """Verify that a user has access to a brand."""
        cursor = mysql.connection.cursor()
        try:
            cursor.execute(
                'SELECT id FROM brand_access WHERE brand_id = %s AND user_id = %s',
                (brand_id, user_id)
            )
            result = cursor.fetchone()
            return result is not None
        except Exception as e:
            print(f'Error verifying brand ownership: {str(e)}')
            return False
        finally:
            cursor.close()

    @staticmethod
    def get_product_by_uid(uid: str, brand_id: int = None) -> dict:
        """Retrieve product details by uid with optional brand verification."""
        cursor = mysql.connection.cursor()
        product = None
        try:
            if brand_id:
                cursor.execute('''
                    SELECT uid, brand_id, title, description, vendor, product_type, tags,
                           status, price, compare_at_price, sku, barcode, weight, weight_unit,
                           collections, brand_color, product_remark, series_length_ankle,
                           series_rise_waist, series_knee, gender, fit_type, print_type,
                           material, material_composition, care_instruction, art_technique,
                           stitch_type, created_at, updated_at
                    FROM fashion
                    WHERE uid = %s AND brand_id = %s
                ''', (uid, brand_id))
            else:
                cursor.execute('''
                    SELECT uid, brand_id, title, description, vendor, product_type, tags,
                           status, price, compare_at_price, sku, barcode, weight, weight_unit,
                           collections, brand_color, product_remark, series_length_ankle,
                           series_rise_waist, series_knee, gender, fit_type, print_type,
                           material, material_composition, care_instruction, art_technique,
                           stitch_type, created_at, updated_at
                    FROM fashion
                    WHERE uid = %s
                ''', (uid,))

            result = cursor.fetchone()
            if result:
                product = {
                    'uid': result[0],
                    'brand_id': result[1],
                    'title': result[2],
                    'description': result[3],
                    'vendor': result[4],
                    'product_type': result[5],
                    'tags': result[6],
                    'status': result[7],
                    'price': result[8],
                    'compare_at_price': result[9],
                    'sku': result[10],
                    'barcode': result[11],
                    'weight': result[12],
                    'weight_unit': result[13],
                    'collections': result[14],
                    'brand_color': result[15],
                    'product_remark': result[16],
                    'series_length_ankle': result[17],
                    'series_rise_waist': result[18],
                    'series_knee': result[19],
                    'gender': result[20],
                    'fit_type': result[21],
                    'print_type': result[22],
                    'material': result[23],
                    'material_composition': result[24],
                    'care_instruction': result[25],
                    'art_technique': result[26],
                    'stitch_type': result[27],
                    'created_at': result[28],
                    'updated_at': result[29]
                }
        except Exception as e:
            print(f'Error fetching product: {str(e)}')
        finally:
            cursor.close()
        return product

    @staticmethod
    def list_products(brand_id: int, limit: int = 50, offset: int = 0, status: str = None, search: str = None) -> list:
        """List products for a brand with optional filtering."""
        cursor = mysql.connection.cursor()
        try:
            where_clauses = ["f.brand_id = %s"]
            params = [brand_id]

            if status:
                where_clauses.append("f.status = %s")
                params.append(status.upper())

            if search:
                where_clauses.append("(f.title LIKE %s OR f.vendor LIKE %s OR f.sku LIKE %s)")
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])

            query = f"""
                SELECT f.uid, f.brand_id, f.title, f.price, f.vendor, f.status,
                       f.created_at, COUNT(DISTINCT li.id) as image_count
                FROM fashion f
                LEFT JOIN low_resol_images li ON f.uid = li.uid
                WHERE {' AND '.join(where_clauses)}
                GROUP BY f.uid
                ORDER BY f.created_at DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limit, offset])
            cursor.execute(query, tuple(params))

            results = cursor.fetchall()
            return [
                {
                    'uid': row[0],
                    'brand_id': row[1],
                    'title': row[2],
                    'price': row[3],
                    'vendor': row[4],
                    'status': row[5],
                    'created_at': row[6],
                    'images_count': row[7]
                }
                for row in results
            ]

        except Exception as e:
            print(f'Error listing products: {str(e)}')
            return []
        finally:
            cursor.close()


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

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing stores table: {str(e)}')

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
            )
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
            )
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
            )
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
            )
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
            )
        ''')

        # Create hsn_record table - HSN/tax classification
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hsn_record (
                hsn VARCHAR(10) PRIMARY KEY,
                uid VARCHAR(36) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_uid (uid),
                FOREIGN KEY (uid) REFERENCES uid_record(uid) ON DELETE CASCADE
            )
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
            )
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
            )
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
            )
        ''')

        mysql.connection.commit()

    except Exception as e:
        mysql.connection.rollback()
        print(f'Error initializing fashion tables: {str(e)}')

    finally:
        if cursor:
            cursor.close()