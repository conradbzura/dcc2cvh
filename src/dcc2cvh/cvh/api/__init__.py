import os
from typing import Final

from motor.motor_asyncio import AsyncIOMotorDatabase

DATABASE_URL: Final = os.getenv("DATABASE_URL", "mongodb://0.0.0.0:27017")
DATABASE_NAME: Final = os.getenv("DATABASE_NAME", "c2m2-backup")
PAGE_SIZE: Final = 25

db: AsyncIOMotorDatabase | None = None
