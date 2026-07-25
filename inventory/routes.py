from quart import Blueprint
from inventory.supplier.api import supplier
from inventory.warehouse.api import warehouse
from inventory.stocks.api import stocks
from inventory.inward.api import inward


inventory = Blueprint("inventory", __name__, url_prefix="/inventory")
inventory.register_blueprint(supplier)
inventory.register_blueprint(warehouse)
inventory.register_blueprint(stocks)
inventory.register_blueprint(inward)