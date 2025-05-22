import strawberry
from typing import NewType
from bson import ObjectId
from dcc2cvh.cvh.models import FileMetadataModel
from pydantic import BaseModel
from typing import get_type_hints

ObjectIdScalar = strawberry.scalar(
    NewType("ObjectIdScalar", str),
    serialize=lambda v: str(v),
    parse_value=lambda v: ObjectId(v),
)


def is_pydantic_model(annotation):
    if issubclass(annotation, BaseModel):
        return True
    elif subtypes := getattr(annotation, "__args__", None):
        return any(issubclass(subtype, BaseModel) for subtype in subtypes)


def build_strawberry_type(type):
    @strawberry.experimental.pydantic.type(model=type)
    @annotate(type, type.__name__)
    class T: ...

    return T


def annotate(model, name=None):
    def wrapper(type):
        if name:
            type.__name__ = f"{name}Type"
        for field_name, field_type in get_type_hints(model).items():
            if not is_pydantic_model(field_type):
                if field_type is ObjectId:
                    type.__annotations__[field_name] = ObjectIdScalar
                else:
                    type.__annotations__[field_name] = field_type
            else:
                if issubclass(field_type, BaseModel):
                    T = build_strawberry_type(field_type)

                    type.__annotations__[field_name] = T
                elif subtypes := getattr(field_type, "__args__", None):
                    for subtype in subtypes:
                        if issubclass(subtype, BaseModel):
                            T = build_strawberry_type(subtype)

                            _T = field_type.__origin__[
                                T,
                                *(t for t in subtypes if t is not subtype),
                            ]
                            type.__annotations__[field_name] = _T
                            break
        return type

    return wrapper


@strawberry.experimental.pydantic.type(model=FileMetadataModel)
@annotate(FileMetadataModel)
class FileMetadataType: ...
