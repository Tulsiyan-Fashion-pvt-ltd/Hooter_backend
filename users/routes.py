from quart import Blueprint
from .authentication.api import auth
from .profile.api import profile


users = Blueprint("user", __name__, url_prefix = "/users")

users.register_blueprint(auth)
users.register_blueprint(profile)