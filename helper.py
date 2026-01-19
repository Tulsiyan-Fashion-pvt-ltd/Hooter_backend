import uuid
import datetime
import re
import hashlib

class User:
    
    @staticmethod
    def create_userid() -> str:
        # create hooter user ids-
        prefix = 'user_'
        unique_id = str(uuid.uuid4())[:18]
        date = str(datetime.datetime.now().date()).replace('-', '')
        userid = prefix+unique_id+date
        return userid
    
    @staticmethod
    def hash_password(password):
        encoded_password = password.encode()
        hash_object = hashlib.sha256(encoded_password)
        hashed_password = hash_object.hexdigest()
        return hashed_password
    

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




if __name__ == "__main__":
    print(User.hash_password('962412'))