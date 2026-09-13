"""mongo_migration takes the collections from the folder name from database_schema/mongodb
the files those folder would be documents in those collections

like /image_schema/default would have default document in the image_chema collection
"""



from pymongo import MongoClient
from pathlib import Path
import aiofiles
import json
import logging
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()


async def migrate():
    mongo = MongoClient(os.environ.get('MONGO_ROUTE'), serverSelectionTimeoutMS=5000)

    path = './mongodb/migration'
    dir = Path(path)

    for folder in dir.iterdir():
        if not folder.is_dir():
            continue

        for file in folder.glob("*.json"):
            async with aiofiles.open(file, "r") as f:
                document = json.loads(await f.read())

                if document:
                    try:
                        await asyncio.to_thread(mongo.db[folder.name].insert_many(document))
                        logging.info(f"Migration completed {folder.name} collection")
                    except Exception as e:
                        print(e)
                        logging.error(f"Migration failed for {folder.name}")


if __name__ == "__main__":
    asyncio.run(migrate())