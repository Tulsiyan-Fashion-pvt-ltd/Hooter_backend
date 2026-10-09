"""
Development application servers the api docs for the Hooter application

"""
from quart import Quart, request, render_template, session, abort, redirect
from dotenv import load_dotenv
import os
from quart_rate_limiter import RateLimiter, RateLimit, rate_limit
from datetime import timedelta
import secrets
from swagger_ui import api_doc
import asyncio
import asyncmy
from asyncmy.cursors import DictCursor
import models
from traceback import print_exc
from werkzeug.security import check_password_hash

'''importing .env'''
load_dotenv()





async def rate_limit_key():
    """Generates key based upon the user session if not then client IP address"""
    if session.get('user'):
        return f"user:{session.get('user')}"      # logged-in user

    return f"ip:{request.headers.get('CF-Connecting-IP', request.remote_addr)}"


limiter = RateLimiter(key_function=rate_limit_key,
                    default_limits=[
                        RateLimit(300, timedelta(minutes=1))
                    ]
            )



app = Quart(__name__)

'''security stuff'''
app.config["SECRET_KEY"] = os.environ.get('SECRET_KEY')
limiter.init_app(app)


# creating and closing of the connection pool
@app.before_serving
async def sql_connection_startup():
    connection = False
    count = 0
    while connection == False and count <= 20:
        try:
            app.pool= await asyncmy.create_pool(
                host = os.environ.get('Employee_DB_HOST'),
                port = int(os.environ.get('Employee_DB_PORT')),
                user = os.environ.get('Employee_DB_USER'),
                password = os.environ.get('Employee_DB_PASSWORD'),
                db = os.environ.get('Employee_DB'),
                minsize = 1,
                maxsize = 20,
                # autocommit=True
                # pool_recycle=3600
            )
            connection = True
        except Exception as e:
            connection = False
            count += 1
            print(e)
            await asyncio.sleep(2)


@app.after_serving
async def sql_connection_shutdown():
    app.pool.close()
    await app.pool.wait_closed()





@app.get('/')
async def login_page():
    csrf_token = secrets.token_urlsafe(32)
    session["csrf"] = csrf_token

    if session.get('user'):
        return redirect('/docs')
    return await render_template("login-page.html", csrf_token=csrf_token)



@app.post('/')
@rate_limit(5, timedelta(minutes=15))
async def login_validation():
    form_data = await request.form
    csrf_token = form_data.get('csrf-token')
    account_id = form_data.get("accountId")
    password = form_data.get("password")


    '''csrf check'''
    if not secrets.compare_digest(csrf_token, session.get('csrf')):
        return abort(403)

    try:
        user = models.UserModel(account_id=account_id, password=password)
    except ValueError as e:
        print(e)
        print(print_exc)
        return e.errors()

    '''sql fetch password for the account_id'''
    pool = app.pool
    async with pool.acquire() as connection:
        try:
            async with connection.cursor(cursor=DictCursor) as cursor:
                q = '''Select a.password_encrypted from 
                account as a
                inner join  openapi_docs as o on o.account_id = a.account_id
                where o.account_id = %s'''
                values = (account_id, )

                await cursor.execute(q, values)
                hashed_password = await cursor.fetchone()
                hashed_password = hashed_password.get('password_encrypted') if hashed_password else None
        except Exception as e:
            print(e)
            print_exc()

            return abort(500)

    if not hashed_password:
        return "Invalid Account Id"
    elif not await asyncio.to_thread(check_password_hash, hashed_password, user.password):
        return "Incorrect password"
        
    session.clear()
    session['user'] = account_id
    session.permanent = False
    return redirect('/docs')



api_doc(
        app,
        config_path="./openapi.yaml",
        url_prefix="/docs",
        title="Hooter internal APIs",
        editor=True
    )


@app.before_request
async def validate_doc():
    if request.path == "/docs" or request.path.startswith("/docs" + "/"):
        if not session.get('user'):
            return redirect('/')



if __name__ == "__main__":
    app.run(debug=True, port=5503, host="0.0.0.0")