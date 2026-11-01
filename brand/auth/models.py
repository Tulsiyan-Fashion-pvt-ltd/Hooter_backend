from pydantic import BaseModel, Field, field_validator, model_validator, EmailStr
import indiapins
from datetime import datetime
import phonenumbers



class BrandRegisterModel(BaseModel):
    brand: BrandSignupModel
    poc: POCCreateModel


class BrandSignupModel(BaseModel):
    entity_name: str = Field(max_length=255)
    brand_name: str = Field(max_length=128)
    gtin: str|None = Field(max_length=15, default=None)
    plan: str
    address: str
    city: str = Field(max_length=128)
    state: str = Field(max_length=128)
    pincode: int
    estyear: int = Field(le=datetime.now().year)


    @field_validator('pincode')
    @classmethod
    def checkpincode(cls, pincode) -> int:
        if not indiapins.isvalid(str(pincode)):
            raise ValueError('Invalid pincode')

        return pincode


    @model_validator(mode='after')
    def validate_address(self):
        found = False
        for city in indiapins.matching(str(self.pincode)):
            if city.get('Name') == self.city and city.get('State') == self.state:
                found = True
        if found:
            raise ValueError('Invalid city, state for the pincode')

        return self



class POCCreateModel(BaseModel):
    self: bool = False
    name: str| None = Field(max_length=128, default=None)
    number: int| None = None
    email: EmailStr| None = None
    title: str| None = Field(max_length=100, default=None)


    def validate_phonenumber(self, number) -> int:
        if not number:
            raise ValueError('POC Phone number is required')
        
        number = phonenumbers.parse(str(number), "IN")
        if not phonenumbers.is_valid_number(number):
            raise False

        return True


    @model_validator(mode='after')
    def validate_self(self):
        if self.self == True:
            return self
        else:
            if self.name is None or self.number is None or self.email is None:
                raise ValueError('Invalid payload, name, number and email is required for POC')
            else:
                if not self.validate_phonenumber(self.number):
                    raise ValueError('Invalid POC phone number')
                return self
            