from quart import Blueprint, render_template

page = Blueprint('page', __name__,template_folder="templates")

@page.route('/')
async def index():
    return await render_template('index.html')

@page.route('/privacy')
async def privacy_policy():
    return await render_template('privacy-policy.html')