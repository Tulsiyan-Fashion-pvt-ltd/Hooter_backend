"""
WRITTER: FARHAN AHMAD
DATE: 18 OCT 2026

DESC:
    tasks.py is a host for reddis job queue. It is helpful for running background tasks into seperate thread.

NEED:
    Background tasks can be resumed if we use arq in case of a system failure or a server restart and 
    having a seperate execution for background heavy jobs will ease up for the event loop to run more efficiently.

USAGE:
    Whichever background function you want to run. 
        1. Register it here in the functions list inside `WorkSettings` class
        2. enqueue the job using this syntax:
            job = await current_app.arq.enqueue_job(
                "registered_func_name",
                "arguments_to_pass",
                "second_arg_to_pass",
            )

            job_id = job.job_id         #job id
"""


from arq.connections import RedisSettings
import os
from dotenv import load_dotenv
import asyncmy
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

REDIS_SETTINGS = RedisSettings(
        host=os.environ.get('HOOTER_REDIS_HOST'),
        port=int(os.environ.get('HOOTER_REDIS_PORT')),
        password=os.environ.get('HOOTER_REDIS_PASSWORD')
    )


async def startup(ctx):
    connection = False
    count = 0
    while connection == False and count <= 20:
        try:
            ctx['pool'] = await asyncmy.create_pool(
                host = os.environ.get('HOOTER_DB_HOST'),
                port = int(os.environ.get('HOOTER_DB_PORT')),
                user = os.environ.get('HOOTER_DB_USER'),
                password = os.environ.get('HOOTER_DB_PASSWORD'),
                db = os.environ.get('HOOTER_DB'),
                minsize = 1,
                maxsize = 20,
                autocommit=True,
                pool_recycle=28800
            )
            connection = True
        except Exception as e:
            connection = False
            count += 1
            print(e)
            await asyncio.sleep(2)

    try:
        ctx['mongo'] = AsyncIOMotorClient(os.environ.get('MONGO_ROUTE'))
    except Exception as e:
        print(e)
        raise Exception('Mongo db connection failed')


async def shutdown(ctx):
    ctx['pool'].close()
    await ctx['pool'].wait_closed()
    ctx['mongo'].close()




# functions
from catalog.products.tasks import upload_xlsx








class WorkerSettings:
    functions = [upload_xlsx]
    redis_settings = REDIS_SETTINGS

if __name__ == "__main__":
    print(os.environ.get('HOOTER_REDIS_PORT'))