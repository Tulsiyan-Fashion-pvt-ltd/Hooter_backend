# this file initialises the mysql db gives mysql object which other files can import and take use of

from flask_mysqldb import MySQL
from flask import session
from datetime import datetime
from pymongo import MongoClient


mysql = MySQL()

def __init_sql__(app):
    mysql.init_app(app)
    print('sql initialized')


# setting up the mongodb
mongodb = MongoClient("mongodb://localhost:27017/")