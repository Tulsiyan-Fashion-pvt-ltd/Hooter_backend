from flask import Flask
from flask_cors import CORS
from pages import page # importing the page blueprint for the page routes
from user_handler import handle_user #importing the request blueprint from requests
from database import __init_sql__
import os
from brand_handler import brand

app = Flask(__name__)
CORS(app, supports_credentials=True,
    origins=["http://localhost:5173"])

app.secret_key = os.environ.get('HOOTER_SECRET_KEY')

app.config['MYSQL_HOST'] = os.environ.get('HOOTER_DB_HOST')
app.config['MYSQL_PORT'] = int(os.environ.get('HOOTER_DB_PORT'))
app.config['MYSQL_USER'] = os.environ.get('HOOTER_DB_USER')
app.config['MYSQL_PASSWORD'] = os.environ.get('HOOTER_DB_PASSWORD')
app.config['MYSQL_DB'] = os.environ.get('HOOTER_DB')

app.register_blueprint(page)
app.register_blueprint(handle_user)
app.register_blueprint(brand)


__init_sql__(app) #initializing sql

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8800)