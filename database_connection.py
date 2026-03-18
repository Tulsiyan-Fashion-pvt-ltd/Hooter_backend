# this file initialises the mysql db gives mysql object which other files can import and take use of

# from flask_mysqldb import MySQL
from quart import current_app
import asyncmy
import os
from dotenv import load_dotenv

load_dotenv() # to load the .env file

# mysql = MySQL()

# def __init_sql__(app):
#     mysql.init_app(app)
#     print('initialized the sql')


#initializing mongodb
# using current_app.mongo to get the mongo object
# mongo = current_app.mongo
# await mongo.db.collection.operation()