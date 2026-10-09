from pydantic import BaseModel, Field


class UserModel(BaseModel):
    account_id: str= Field(pattern=r"^H0\d+$")
    password: str


if __name__ == "__main__":
    user =  UserModel(account_id = "H020240801", password="apnapassword")

    print(user)