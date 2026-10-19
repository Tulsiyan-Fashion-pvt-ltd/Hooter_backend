from quart import Blueprint
from .auth.api import auth
from .profile.api import brand_profile


brand = Blueprint("brand", __name__, url_prefix = "/brand")


brand.register_blueprint(auth)
brand.register_blueprint(brand_profile)