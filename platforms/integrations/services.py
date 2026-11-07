from . import mariadb



async def get_connections(brand_id: str):
    """
    Get connection status of all the integrated platforms
    """
    # shopify = await shopify_account(brand_id)
    # return shopify[0],  200
    connections = await mariadb.Fetch.brand_connections(brand_id)

    if connections.get('status') == 'failed':
        return {'status': 'failed', 'message': 'Could not fetch the brand connections'}, 500

    else:
        return connections.get('value'), 200