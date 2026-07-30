import uuid
import datetime
import re
import hashlib
import json
    

# class handling all the validating
class Validate:

    @staticmethod
    def email(mail):
        mail_id = mail
        regex_expression = r'^[A-Za-z0-9]+([._%+-]?[A-Za-z0-9]+)*@[A-Za-z0-9-]+(\.[A-Za-z]{2,})+$'
        if re.match(regex_expression, mail_id):
            return True
        else: 
            return False
        
    @staticmethod
    def in_phone_num(number):
        phone_number = str(number)
        phone_number = phone_number.lstrip('+91')
        phone_number = phone_number.replace('-', '')

        regex = r'^\d{10}$'
        if re.match(regex, phone_number.strip()):  #checking the phonenumber by removing the whitespace in case
            return True                             #the number is something like +91 xxxxxxxxxx
        else:
            return False


# creating additional package of almost repeatative tasks
class Helper:
    @staticmethod
    def date():
        date = str(datetime.datetime.now().date())
        return date

    @staticmethod
    def time():
        time = str(datetime.datetime.now().strftime('%H:%M:%S'))
        return time
    
    # if you want to check if the current payload is valid or not
    # create a list of expected payloads
    # and pass the json response payload of the request here
    @staticmethod
    def check_required_payload(payload: dict, accepted_keys: list, necessary_keys: list):
        return (
            all(key in accepted_keys for key in payload) and 
            all(key in payload and payload[key] is not None for key in necessary_keys)
        )

class Brand:
    @staticmethod
    def create_id() -> str:
        prefix = 'brand_'

        unique_id = str(uuid.uuid4())[:14]
        date = str(datetime.datetime.now().date()).replace('-', '')
        id = prefix+unique_id+date
        return id
    


if __name__ == "__main__":
    print(Brand.access_specifiers())