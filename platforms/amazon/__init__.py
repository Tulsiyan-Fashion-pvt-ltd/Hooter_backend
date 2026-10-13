from quart import Blueprint

amazon = Blueprint("amazon", __name__)

from . import auth