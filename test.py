
#!/usr/bin/env python3
from collections import UserDict
from os import environ
from typing import List

from boto3 import resource
from mypy_boto3_dynamodb.service_resource import Table


from dynamojo.db import IndexMap, get_indexes, IndexList, Lsi, Gsi, TableIndex
from dynamojo.base import ObjectBase
from dynamojo.exceptions import IndexNotFoundError
from boto3.dynamodb.conditions import Key

TABLE = resource("dynamodb").Table(environ["DYNAMODB_TABLE"])


class Foo(ObjectBase):
  indexes: List[IndexList] = get_indexes(environ["DYNAMODB_TABLE"])
  _table: Table = TABLE

  index_map = [
    IndexMap(
      index=indexes.table,
      sortkey = "typeNameAndDateSearch",
      partitionkey="accountId"
    ),
    IndexMap(
      index=indexes.gsi0,
      sortkey="dateTime",
      partitionkey="notificationType"
    ),
    IndexMap(
      index=indexes.lsi0,
      sortkey="dateTime",
    )
  ]

  compound_attributes = {
    "typeNameAndDateSearch": [
      "notificationType",
      "notificationName",
      "dateTime"
    ]
  }

  def __init__(
    self,
    **kwargs
  ):
    self._required_attributes = ["notificationType", "notificationName", "dateTime", "accountId", "message"]
    self._optional_attributes = []
    self._static_attributes = []
    super().__init__(**kwargs)


foo = Foo(
  notificationType="TEST_ALARM_TYPE",
  notificationName="notification name test",
  dateTime="3456778554363456",
  accountId="MYACCOUNT_12345",
  message="Test message"
)
foo.save()
condition = Key("accountId").eq("MYACCOUNT_12345") & Key("typeNameAndDateSearch").gt("0")
res = Foo.query(condition=condition)
print(res)
#print(Foo().get_index().name)
exit()
foo.save()

#foo.save()
#print(Foo.fetch("foo", "Foo~foo"))
#res = Foo.query(indexes.table.begins_with("foo", "Foo~"))
#print(res)

"""

from abc import ABC, abstractproperty
class Base(UserDict, ABC):

  @abstractproperty
  def _required_fields(self):
    pass

class Foo(Base):

  def _required_fields(self):
    return []

  def __init__(self, **kwargs):
    super().__init__(**kwargs)
    self.foo = "bar"
    self.data["data_item"] = "some data"

  def __getattribute__(self, __name: str):
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      return self.data[__name]


foo = Foo()
print(foo.data_item)
"""
