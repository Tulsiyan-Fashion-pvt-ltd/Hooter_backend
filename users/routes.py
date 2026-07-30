from quart import Blueprint
from .authentication.api import auth


user = Blueprint("user", __name__, url_prefix = "/users")

user.register_blueprint(auth)