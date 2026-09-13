import uuid
import datetime

class Brand:
    @staticmethod
    def create_id() -> str:
        prefix = 'brand_'

        unique_id = str(uuid.uuid4())[:14]
        date = str(datetime.datetime.now().date()).replace('-', '')
        id = prefix+unique_id+date
        return id