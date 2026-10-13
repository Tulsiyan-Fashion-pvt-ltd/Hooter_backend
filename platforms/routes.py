from quart import Blueprint
from .shopify.routes import shopify
from .amazon.routes import amazon
from .flipkart.routes import flipkart



platforms = Blueprint("platforms", __name__, url_prefix = "/platforms")

platforms.register_blueprint(shopify)
platforms.register_blueprint(amazon)
platforms.register_blueprint(flipkart)