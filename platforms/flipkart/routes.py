from quart import Blueprint
from .auth.api import auth

flipkart = Blueprint("flipkart", __name__, url_prefix="/flipkart")

flipkart.register_blueprint(auth)