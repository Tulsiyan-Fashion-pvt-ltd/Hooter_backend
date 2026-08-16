from pydantic import BaseModel, field_validator
from config import _image_types

class Get_image(BaseModel):
    image_type: str
    image_key: str

    @field_validator("image_type")
    def check_image_type(cls, image_type):
        if image_type not in _image_types:
            raise ValueError("image must be of valid type")
        return image_type