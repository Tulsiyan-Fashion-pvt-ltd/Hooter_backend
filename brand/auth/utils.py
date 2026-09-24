import uuid
import datetime


def create_id() -> str:
    '''Create unique 36 bytes brand id'''
    prefix = 'brand-'
    unique_id = str(uuid.uuid4())[:14]
    date = str(datetime.datetime.now().date()).replace('-', '')
    id = prefix+unique_id+date
    return id