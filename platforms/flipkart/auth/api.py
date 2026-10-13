#the url for this path is /platforms/flipkart/auth

from quart import Blueprint
from utils.prerequirements import login_required, brand_required


auth = Blueprint('flipkart_auth', __name__, url_prefix="/auth")


@auth.get('/authorize')
@login_required
@brand_required
async def authorize_flipkart_account():
    ...