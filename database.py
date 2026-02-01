from flask_mysqldb import MySQL
from flask import session

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('sql initialized')

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
    def user_access(user_id):
        user = user_id
        user_access=None
        cursor = mysql.connection.cursor()
        try:
            cursor.execute('''
                            select user_access 
                           from user_creds
                           where user_id=%s 
                        ''', (user_id, ))

            user_access = cursor.fetchone()
            user_access = user_access[0]
        except Exception as e:
            print(f'error encountered while fetching the user access\n{e}')
        finally:
            cursor.close()
        return user_access