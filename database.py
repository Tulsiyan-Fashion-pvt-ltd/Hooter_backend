from flask_mysqldb import MySQL

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('sql initialized')
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


def init_catalogue_tables(app):
    """Initialize catalogue and Shopify mapping tables."""
    cursor = None
    try:
        cursor = mysql.connection.cursor()

        # Create catalogue table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue (
                catalogue_id VARCHAR(36) PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description LONGTEXT NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                user_id VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_created_at (created_at)
            )
        ''')

        # Create Shopify mapping table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogue_shopify_mapping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                catalogue_id VARCHAR(36) NOT NULL UNIQUE,
                shopify_product_id VARCHAR(255) NOT NULL UNIQUE,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_catalogue_id (catalogue_id),
                INDEX idx_shopify_product_id (shopify_product_id),
                FOREIGN KEY (catalogue_id) REFERENCES catalogue(catalogue_id) ON DELETE CASCADE
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