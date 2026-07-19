from quart import Blueprint
from inventory.supplier.routes import supplier
from inventory.warehouse.routes import warehouse
from inventory.stocks.routes import stocks
from inventory.inward.routes import inward


inventory = Blueprint("inventory", __name__, url_prefix="/inventory")
inventory.register_blueprint(supplier)
inventory.register_blueprint(warehouse)
inventory.register_blueprint(stocks)
inventory.register_blueprint(inward)