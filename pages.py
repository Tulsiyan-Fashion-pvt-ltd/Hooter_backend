from quart import Blueprint, render_template, request

page = Blueprint('page', __name__)

@page.route('/')
async def index():
    return await render_template('index.html')