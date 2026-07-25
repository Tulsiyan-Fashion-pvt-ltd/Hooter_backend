from quart import Blueprint
from .auth.api import auth

brand = Blueprint("brand", __name__, url_prefix = "/brand")

brand.register_blueprint(auth)