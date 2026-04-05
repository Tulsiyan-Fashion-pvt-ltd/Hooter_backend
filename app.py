from quart import Quart
from quart_cors import cors
from pages import page # importing the page blueprint for the page routes
from api_routes.user_routes import handle_user #importing the request blueprint from requests
# from database_connection import __init_sql__, __init_mongodb__
import os
from api_routes.brand_routes import brand
# from shopify.products import products8800
from shopify.stores import stores
from api_routes import catalog_routes
from dotenv import load_dotenv
import asyncmy
from datetime import timedelta
from quart_mongo import Mongo
import asyncio


load_dotenv()  # Load environment variables from .env file

app = Quart(__name__)
cors(app, allow_credentials=True,
    allow_origin=['http://192.168.1.26:5173', 'http://127.0.0.1:5173', 'http://localhost:5173', 'https://workspace.h0oter.com', 'https://staging_workspace.h0oter.com'],
    # send_origin_wildcard=False,
    max_age=timedelta(days=1))

app.secret_key = os.environ.get('HOOTER_SECRET_KEY')
app.config["SECRET_KEY"] = os.environ.get('HOOTER_SECRET_KEY')

# only for texting nad development
app.config["SESSION_COOKIE_SAMESITE"] = "None"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get('SESSION_COOKIE_SECURE')  # because you're using http locally

app.config['MYSQL_HOST'] = os.environ.get('HOOTER_DB_HOST')
app.config['MYSQL_PORT'] = int(os.environ.get('HOOTER_DB_PORT'))
app.config['MYSQL_USER'] = os.environ.get('HOOTER_DB_USER')
app.config['MYSQL_PASSWORD'] = os.environ.get('HOOTER_DB_PASSWORD')
app.config['MYSQL_DB'] = os.environ.get('HOOTER_DB')
app.config['MYSQL_PORT'] = int(os.environ.get('HOOTER_DB_PORT', '3306'))

# mongo db connection
app.config['MONGO_URI'] = os.environ.get('MONGO_HOST')
app.mongo = Mongo(app)

app.config["IMAGE_READ_BUFFER"] = (64 * 1024) # 64 KB 

app.register_blueprint(page)
app.register_blueprint(handle_user)
app.register_blueprint(brand)
app.register_blueprint(catalog_routes.catalog)
# need to conver the programs and methods as per asgi
# app.register_blueprint(products)
# app.register_blueprint(stores)


# creating and closing of the connection pool
@app.before_serving
async def sql_connection_startup():
    connection = False
    count = 0
    while connection == False and count <= 20:
        try:
            app.pool= await asyncmy.create_pool(
                host = os.environ.get('HOOTER_DB_HOST'),
                port = int(os.environ.get('HOOTER_DB_PORT')),
                user = os.environ.get('HOOTER_DB_USER'),
                password = os.environ.get('HOOTER_DB_PASSWORD'),
                db = os.environ.get('HOOTER_DB'),
                minsize = 1,
                # maxsize = 20,
                pool_recycle=3600
            )
            connection = True
        except Exception as e:
            connection = False
            count += 1
            print(e)
            await asyncio.sleep(2)


@app.after_serving
async def sql_connection_shutdown(response):
    app.pool.close()
    await app.pool.wait_closed()


if __name__ == "__main__":
    print('''>>>\nuse @login_required when the login is required and use\nfrom utils.prerequirements import login_required''')
    app.run(debug=True, host='0.0.0.0', port=8800)