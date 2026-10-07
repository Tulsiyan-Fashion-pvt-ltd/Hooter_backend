from quart import Blueprint
from .auth.api import auth


shopify = Blueprint("shopify", __name__, url_prefix = "/shopify")

shopify.register_blueprint(auth)