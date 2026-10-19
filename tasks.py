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

load_dotenv()





class WorkerSettings:
    functions = []
    redis_settings = RedisSettings(
        host=os.environ.get('HOOTER_REDIS_HOST'),
        port=os.environ.get('HOOTER_REDIS_PORT'),
        password=os.environ.get('HOOTER_REDIS_PASSWORD')
    )