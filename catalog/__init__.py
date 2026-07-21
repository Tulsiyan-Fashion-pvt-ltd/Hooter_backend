from quart import Blueprint
from .categories import categories

catalog = Blueprint("catalog", __name__, url_prefix = "/catalog")

catalog.register_blueprint(categories)