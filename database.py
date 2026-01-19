from flask_mysqldb import MySQL

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('sql initialized')

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
        print(f'error encounetered while signing up the user as {e}\n sql rollback')
        return 'error'
    finally:
        cursor.close()
    return 'ok'

