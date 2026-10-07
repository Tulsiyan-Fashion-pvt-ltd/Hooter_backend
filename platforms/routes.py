from quart import Blueprint
from .shopify.routes import shopify



platforms = Blueprint("platforms", __name__, url_prefix = "/platforms")

platforms.register_blueprint(shopify)