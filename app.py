from quart import Quart
# from asgiref.wsgi import WsgiToAsgi
from quart_cors import cors
from pages import page # importing the page blueprint for the page routes
from handle_user.user_handler import handle_user #importing the request blueprint from requests
from database import __init_sql__, create_pool, close_pool
import os
from handle_brand.brand_handler import brand
from shopify.products import products
from shopify.stores import stores
from dotenv import load_dotenv
import asyncmy

load_dotenv()  # Load environment variables from .env file

app = Quart(__name__)
cors(app, allow_credentials=True,
    allow_origin=["http://localhost:5173", "http://127.0.0.1:5173", "https://workspace.h0oter.com"])

app.secret_key = os.environ.get('HOOTER_SECRET_KEY')
app.config["SECRET_KEY"] = os.environ.get('HOOTER_SECRET_KEY')

# only for texting nad development
app.config["SESSION_COOKIE_SAMESITE"] = "None"
app.config["SESSION_COOKIE_SECURE"] = False  # because you're using http locally

app.config['MYSQL_HOST'] = os.environ.get('HOOTER_DB_HOST')
app.config['MYSQL_PORT'] = int(os.environ.get('HOOTER_DB_PORT'))
app.config['MYSQL_USER'] = os.environ.get('HOOTER_DB_USER')
app.config['MYSQL_PASSWORD'] = os.environ.get('HOOTER_DB_PASSWORD')
app.config['MYSQL_DB'] = os.environ.get('HOOTER_DB')
app.config['MYSQL_PORT'] = int(os.environ.get('HOOTER_DB_PORT', '3306'))

app.register_blueprint(page)
app.register_blueprint(handle_user)
# app.register_blueprint(brand)
# app.register_blueprint(products)
# app.register_blueprint(stores)


# creating and closing of the connection pool
@app.before_serving
async def sql_connection_startup():
    app.pool= await asyncmy.create_pool(
        host = os.environ.get('HOOTER_DB_HOST'),
        port = int(os.environ.get('HOOTER_DB_PORT')),
        user = os.environ.get('HOOTER_DB_USER'),
        password = os.environ.get('HOOTER_DB_PASSWORD'),
        db = os.environ.get('HOOTER_DB'),
        minsize = 1,
        maxsize = 20
    )


@app.after_serving
async def sql_connection_shutdown(response):
    await close_pool()


# asgi_app = WsgiToAsgi(app)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8800)