from quart import Blueprint
from .categories import categories
from .products import products
from .images import images

catalog = Blueprint("catalog", __name__, url_prefix = "/catalog")

catalog.register_blueprint(categories)
catalog.register_blueprint(products)
catalog.register_blueprint(images)