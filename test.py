
from dynamojo.db import IndexMap, get_indexes
from dynamojo.base import ObjectBase
from os import environ
from boto3 import resource

TABLE = resource("dynamodb").Table(environ["DYNAMODB_TABLE"])

indexes = get_indexes(environ["DYNAMODB_TABLE"])

class Foo(ObjectBase):

  INDEXES = indexes
  INDEX_MAP = [
    IndexMap(
      index=indexes.table,
      sortkey=["objectType", "name"],
      partitionkey="name"
    ),
    IndexMap(
      index=indexes.gsi0,
      sortkey="name",
      partitionkey="objectType"
    )
  ]

  def __init__(
    self,
    **kwargs
  ):
    super().__init__(
      required_attributes=["name"],
      optional_attributes=[],
      static_attributes=["name"],
      **kwargs
    )

foo = Foo(name="foo")
print(foo)
foo.save()
print(Foo.fetch("foo", "Foo~foo"))
res = Foo.query(indexes.table.begins_with("foo", "Foo~"))
print(res)