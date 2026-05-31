import os
from dotenv import load_dotenv
from db.db_connections import user_collection
from db.mongodb.user_repo import MongoUserRepository
# import future PostgreSQL/MySQL repos here

load_dotenv()
ENGINE = os.getenv('DBENGINE')

if not ENGINE:
    raise ValueError("FATAL ERROR: 'ENGINE' is missing in .env file.")

class DatabaseFactory:
    def __init__(self):
        self.db_engine = os.getenv(ENGINE, "mongodb").lower()
        
        if self.db_engine == "mongodb":
            self.user_repo = MongoUserRepository(user_collection)
            
        elif self.db_engine == "postgres":
            # self.user_repo = PostgresUserRepository(user_collection)
            pass
        else:
            raise ValueError(f"Unsupported database engine: {self.db_engine}")
        
    async def initialize_all(self):
        """Initializes all repositories based on the active DB engine."""
        print(f"Initializing {self.db_engine} database components...")
        await self.user_repo.initialize()

db = DatabaseFactory()