import redis.asyncio as redis
import os
from dotenv import load_dotenv
from traceback import print_exc

load_dotenv()

r = redis.Redis(
    host="localhost", 
    port= int(os.environ.get("HOOTER_REDIS_PORT")), 
    password=os.environ.get("HOOTER_REDIS_PASSWORD"),
    decode_responses=True
)


async def store(key: str| int, value: any) -> bool:
    """It stores the the data in the redis in key, value pair. 
    The key and value eventually get pass in redis in string so the arguments should be serialized.
    
    Parameters:
        key: key of the value
        value : value of the given key
    
    Returns:
        - `True` on success
        - `False` on failure
    """
    global r
    try:
        await r.set(f"{key}", f"{value}")
        return True
    except Exception as e:
        print_exc()
        print(e)
        return False
    


async def get(key: str) -> str| bool:
    """It retrieves the the data from the redis as a string or seriealised form of data. 
    The key and value eventually get pass in redis in string so the argument should be serialized.
    
    Parameters:
        key: key of the value
    
    Returns:
        value:
            - string/ serialized data
            - `False` on failure
    """
    global r
    try:
        value = await r.get(f"{key}")
        return value
    except Exception as e:
        print_exc()
        print(e)
        return False
    


async def delete(key: str| int) -> bool:
    """It deletes the the data from the redis. 
    The key and value eventually get pass in redis in string so the argument should be serialized.
    
    Parameters:
        key: key of the value
    
    Returns:
        - `True` on success
        - `False` on failure
    """
    global r
    try:
        await r.delete(f"s{key}")
        return True
    except Exception as e:
        print_exc()
        print(e)
        return False
    


async def set_many(object: dict) -> bool:
    """It sets key, value pair data from the object to the redis in the key value pair where the keys of 
        the object are the keys for redis and same goes for their values. 
        The key and value eventually get pass in redis in string so keys, values should be 
        serialized.
        
        Parameters:
            object: key, value pair
        
        Returns:
            - `True` on success
            - `False` on failure
    """
    global r
    try:
        if not object or type(object) != dict:
           raise ValueError

        await r.mset({
            f"{key}": f"{object.get(key)}"
            for key in object if type(object) is dict
        })

        return True
    except Exception as e:
        print_exc()
        print(e)



async def get_many(keys: list) -> list:
    """It fetches values from the redis in a the list.
        The key and value eventually get pass in redis in string so the keys inside the list should be
        serialized.
        
        Parameters:
            keys: list of keys 
        
        Returns:
            - value: list of values in the same order
            - `False` on failure
    """
    global r
    try:
        if type(keys) != list:
            raise ValueError


        values = await r.mget(
            f"{key}"
            for key in keys if keys
        )

        return values
    except Exception as e:
        print_exc()
        print(e)
        return False



async def delete_many(keys: list) -> bool:
    """It deletes keys, values from the redis.
        The key and value eventually get pass in redis in string so the keys inside the list should be
        serialized.
        
        Parameters:
            keys: list of keys 
        
        Returns:
            - `True`  | on success
            - `False` |  on failure
    """
    global r
    try:
        if type(keys) != list:
            raise ValueError

        await r.delete(f"{key}" for key in keys)
        return True
    except Exception as e:
        print_exc()
        print(e)
        return False


if __name__ == "__main__":
    import asyncio

    async def check_connection():
        try:
          # Ping the Redis server
          response = await r.ping()
          if response:
            print("Successfully connected to Redis!")
        except Exception as e:
          print(f"Failed to connect: {e}")


    asyncio.run(check_connection())