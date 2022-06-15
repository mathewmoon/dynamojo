from pydantic import BaseModel, validator

from abc import ABC, abstractclassmethod, abstractproperty
from collections import UserDict
from datetime import datetime

from typing import (
  Any,
  ClassVar,
  Dict,
  List,
  Optional
)
from os import environ
from typing import List
from typing import TYPE_CHECKING
from boto3 import resource
from mypy_boto3_dynamodb.service_resource import Table


from dynamojo.index import IndexMap, get_indexes, Index
from dynamojo.base import DynamojoBase, Mutator
from dynamojo.config import DynamojoConfig
from boto3.dynamodb.conditions import Key

if not TYPE_CHECKING:
  Table = object

TABLE = resource("dynamodb").Table("account-events")
indexes = get_indexes("account-events")

def mutate_account(source, value, obj):
  return "%".join([value, obj.dateTime])

class Foo(DynamojoBase):
  accountId: str
  dateTime: str
  notificationType: str
  notificationName: str

  class DynamojoConfig:
    indexes: ClassVar = indexes
    index_maps: ClassVar = [
        IndexMap(
            index=indexes.table,
            sortkey="typeNameAndDateSearch",
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
    table: ClassVar = TABLE
    #compound_attributes: ClassVar = {
    #    "typeNameAndDateSearch": [
    #        "notificationType",
    #        "notificationName"
    #    ]
    #}
    static_attributes: ClassVar = ["dateTime", "accountId"]
    mutators = [
      Mutator(source="accountId", target="notificationName", callable=mutate_account)
    ]

  def __init__(self, **kwargs):
    super().__init__(**kwargs)

dt = datetime.now().isoformat()

foo = Foo(
  accountId="abcd1234kdhfg",
  dateTime=dt,
  notificationName="TestName",
  notificationType="ALARM"
)

print(foo.item)
exit()
foo.save()

res = Foo.fetch(foo.accountId, foo.typeNameAndDateSearch)
print(res.item)
exit()
res = Foo.query(
  Key("accountId").eq("abcd1234kdhfg")
)


print(res)

for item in res["Items"]:
  item.delete()
