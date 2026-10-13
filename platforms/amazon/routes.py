from quart import Blueprint
from .auth.api import auth


amazon = Blueprint("amazon", __name__, url_prefix="/amazon")

amazon.register_blueprint(auth)