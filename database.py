from flask_mysqldb import MySQL

mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('sql initialized')

def signup_user(**kwargs):
    cursor = mysql.connection.cursor()
    try:
        userid = kwargs.get('userid')
        hashed_password = kwargs.get('hashed_password')
        number = kwargs.get('number')
        email = kwargs.get('email')
        designation = kwargs.get('designation')


        cursor.execute('''insert into users(user_id, user_password)
                       values(%s, %s)
                       ''', (userid, hashed_password))
        
        cursor.execute('''insert into user_creds(user_id, phone_number, user_email, user_access, user_designation)
                       values(%s, %s, %s, %s, %s)
                        ''', (userid, number, email, 'super_user', designation))
        
        mysql.connection.commit()
    except Exception as e:
        mysql.connection.rollback()
        print(f'error encounetered while signing up the user as {e}\n sql rollback')
        return 'error'
    finally:
        cursor.close()
    return 'ok'

