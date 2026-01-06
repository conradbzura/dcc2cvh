import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from strawberry.fastapi import GraphQLRouter

from cfdb import api
from cfdb.api.gql.schema import schema
from cfdb.api.routers.data import router as data_router
from cfdb.api.routers.sync import router as sync_router

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI):
    print(f"Connecting to MongoDB at {api.DATABASE_URL}")
    api.db = (client := AsyncIOMotorClient(api.DATABASE_URL))[api.DATABASE_NAME]
    yield
    client.close()


app = FastAPI(lifespan=lifespan)
app.include_router(GraphQLRouter(schema), prefix="/metadata")
app.include_router(data_router)
app.include_router(sync_router)
