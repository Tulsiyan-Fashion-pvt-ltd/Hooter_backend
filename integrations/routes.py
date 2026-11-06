from quart import Blueprint
from .api import integrations_api

integrations = Blueprint("integrations", __name__, url_prefix="/integrations")

integrations.register_blueprint(integrations_api)