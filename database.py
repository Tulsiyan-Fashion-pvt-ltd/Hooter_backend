from flask_mysqldb import MySQL
from flask import session

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('initialized the sql')

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
            access=user_creds.get('access') if user_creds.get('access') else 'super_user'


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