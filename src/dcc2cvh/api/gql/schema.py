import strawberry
from typing import List, Optional
from dcc2cvh.api.gql.types import (
    ObjectIdScalar,
    FileMetadataType,
)
from dcc2cvh.api.gql.inputs import (
    FileMetadataInput,
    to_dict,
    to_query,
)
from dcc2cvh.models import FileMetadataModel
from dcc2cvh import api
import pprint


def from_pydantic(gql_type, obj):
    if obj is None:
        return obj
    for field_name, field_type in gql_type.__annotations__.items():
        if field_name in obj:
            if hasattr(field_type, "__strawberry_definition__"):
                obj[field_name] = from_pydantic(field_type, obj[field_name])
            elif hasattr(field_type, "of_type") and hasattr(
                field_type.of_type, "__strawberry_definition__"
            ):
                if isinstance(obj[field_name], list):
                    obj[field_name] = [
                        from_pydantic(field_type.of_type, o) for o in obj[field_name]
                    ]
                else:
                    obj[field_name] = from_pydantic(field_type.of_type, obj[field_name])

    return gql_type(**obj)


@strawberry.type
class Query:
    @strawberry.field
    async def files(
        self,
        _: strawberry.Info,
        input: list[FileMetadataInput] | None = None,
        page: int = 0,
        page_size: int = api.PAGE_SIZE,
    ) -> List[FileMetadataType]:
        assert api.db is not None
        query = to_query(to_dict(input)) if input else {}
        print(pprint.pformat(query))
        files = (
            await api.db.files.find(query)
            .skip(page * page_size)
            .limit(page_size)
            .to_list(length=None)
        )
        return [
            from_pydantic(FileMetadataType, FileMetadataModel(**file).model_dump())
            for file in files
        ]

    @strawberry.field
    async def file(
        self, _: strawberry.Info, id: ObjectIdScalar
    ) -> Optional[FileMetadataType]:
        assert api.db is not None
        file = await api.db.files.find_one({"_id": id})
        if file:
            return from_pydantic(
                FileMetadataType, FileMetadataModel(**file).model_dump()
            )
        return None


schema = strawberry.Schema(query=Query)
