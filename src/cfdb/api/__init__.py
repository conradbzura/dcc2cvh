import os
from typing import Final

from motor.motor_asyncio import AsyncIOMotorDatabase

DATABASE_URL: Final = os.getenv("DATABASE_URL", "mongodb://127.0.0.1:27017")
DATABASE_NAME: Final = os.getenv("DATABASE_NAME", "cfdb")
PAGE_SIZE: Final = 25

db: AsyncIOMotorDatabase | None = None
