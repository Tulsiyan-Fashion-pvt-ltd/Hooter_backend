from pydantic import BaseModel, field_validator, model_validator
import indiapins
import phonenumbers

class warehouse(BaseModel):
    alias: str
    name: str
    phone_number: str
    email: str
    address: str| None = None
    city: str
    state: str
    pincode: int


    @field_validator("phone_number")
    @classmethod
    def validate_phonenumber(cls, number):
        parsed_value = phonenumbers.parse(number, "IN")

        if not phonenumbers.is_valid_number(parsed_value):
            raise ValueError("Invalid phone number")

        return number

    @field_validator("pincode")
    @classmethod
    def validate_pincode(cls, value):
        if not indiapins.isvalid(str(value)):
            raise ValueError("Invalid pincode")

        return value

    @model_validator(mode="after")
    def validate_location(self):
        records = indiapins.matching(str(self.pincode))

        # Accept if ANY office for that PIN matches
        valid = any(
            r["State"].lower() == self.state.lower()
            for r in records
        )

        if not valid:
            raise ValueError("State does not match the pincode")

        return self 

    

class warehouse_update(BaseModel):
    alias: str| None = None
    name: str | None = None
    phone_number: str | None = None
    email: str | None = None
    address: str| None = None
    city: str| None = None
    state: str| None = None
    pincode: int | None = None



    @field_validator("phone_number")
    @classmethod
    def validate_phonenumber(cls, number):
        parsed_value = phonenumbers.parse(number, "IN")

        if number and not phonenumbers.is_valid_number(parsed_value):
            raise ValueError("Invalid phone number")

        return number


    @field_validator("pincode")
    @classmethod
    def validate_pincode(cls, value):
        if value and not indiapins.isvalid(str(value)):
            raise ValueError("Invalid pincode")

        return value


    @model_validator(mode="after")
    def validate_location(self):
        records = indiapins.matching(str(self.pincode))

        # Accept if ANY office for that PIN matches
        valid = any(
            r["State"].lower() == self.state.lower()
            for r in records
        )

        if not valid:
            raise ValueError("State does not match the pincode")

        return self