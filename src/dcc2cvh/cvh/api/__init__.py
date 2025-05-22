from motor.motor_asyncio import AsyncIOMotorDatabase

DATABASE_URL = "mongodb://localhost:27017"
DATABASE_NAME = "c2m2-backup"
PAGE_SIZE = 25

db: AsyncIOMotorDatabase | None = None
