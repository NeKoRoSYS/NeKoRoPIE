import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()
URL = os.getenv('DBURI')

if not URL:
    raise ValueError("FATAL ERROR: Environment variable 'DBURI' is not set in .env file.")
        

client = AsyncIOMotorClient(URL)
db = client["db"] # make sure there's a database named "db"
user_collection = db["user"] # inside database "db" there must be a collection called "user"
