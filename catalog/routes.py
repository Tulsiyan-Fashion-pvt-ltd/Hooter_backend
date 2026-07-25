from quart import Blueprint
from .categories.api import categories
from .products.api import products
from .images.api import images

catalog = Blueprint("catalog", __name__, url_prefix = "/catalog")

catalog.register_blueprint(categories)
catalog.register_blueprint(products)
catalog.register_blueprint(images)