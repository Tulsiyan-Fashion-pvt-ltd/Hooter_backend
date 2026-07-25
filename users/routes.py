from quart import Blueprint
from .authentication.api import auth


users = Blueprint("users", __name__, url_prefix = "/users")

users.register_blueprint(auth)