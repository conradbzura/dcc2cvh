from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from dcc2cvh.cvh.api.gql.schema import schema
from dcc2cvh.cvh import api
from dcc2cvh.cvh.api.routers.data import router as data_router
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(_: FastAPI):
    print(f"Connecting to MongoDB at {api.DATABASE_URL}")
    api.db = (client := AsyncIOMotorClient(api.DATABASE_URL))[api.DATABASE_NAME]
    yield
    client.close()


app = FastAPI(lifespan=lifespan)
app.include_router(GraphQLRouter(schema), prefix="/metadata")
app.include_router(data_router)
