from typing import (
  ClassVar,
  Union,
  Callable,
  List,
  Dict,
  TYPE_CHECKING
)

from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel, BaseConfig

from .index import (
  Index,
  IndexList,
  IndexMap,
  Mutator
)

if not TYPE_CHECKING:
  Table = object

class DynamojoConfig(BaseModel):
    # A dictionary in the form of {"<target attribute>": ["source_att_one", "source_att_two"]} where <target_attribute> will
    # automatically be overwritten by the attributes of it's corresponding list being joined with '~'. This is useful for creating
    # keys that can be queried over using Key().begins_with() or Key().between() by creating a means to filter based on the compounded
    # attributes.
    joined_attributes: Dict[str, Union[List[str], Callable]] = {}

    # A list of database `Index` objects from `dynamojo.indexes.get_indexes()``
    indexes: IndexList

    # A list of `IndexMap` objects used to map arbitrary fields into index attributes
    index_maps: List[IndexMap] = []

    static_attributes: List[str] = []

    # A Dynamodb table object
    table: Table

    join_separator: str = "~"

    mutators: List[Mutator] = []

    joined_attributes: Dict[str, List[str]] = {}

    static_attributes: List[str] = []

    underscore_attrs_are_private: bool = True

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"