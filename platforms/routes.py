from quart import Blueprint
from .shopify.routes import shopify
from .amazon.routes import amazon
from .flipkart.routes import flipkart
from .integrations.api import integrations_api



platforms = Blueprint("platforms", __name__, url_prefix = "/platforms")

platforms.register_blueprint(shopify)
platforms.register_blueprint(amazon)
platforms.register_blueprint(flipkart)
platforms.register_blueprint(integrations_api)