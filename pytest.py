from json import dumps
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

#def mutate_sk(_, value, obj):
#  return "~".join([obj.notificationType, value])

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
            sortkey="dateTime"
        )
    ]
    table: ClassVar = TABLE
    joined_attributes: ClassVar = {
      "typeNameAndDateSearch": [
        "notificationType",
        "notificationName",
        "dateTime"
      ]
    }
    static_attributes: ClassVar = ["dateTime", "accountId"]
    mutators = []    #  Mutator(source="dateTime", callable=mutate_sk)


  def __init__(self, **kwargs):
    super().__init__(**kwargs)

dt = datetime.now().isoformat()

"""
for n in range(100):
  Foo(
    accountId="abcd1234kdhfg",
    dateTime=datetime.now().isoformat(),
    notificationName="TestName",
    notificationType="ALARM"
  ).save()
"""
#print(foo.item)
#exit()
#foo.save()

#res = Foo.fetch(foo.accountId, foo.typeNameAndDateSearch)
#print(res.item)
#exit()
res = Foo.query(
    Key("accountId").eq("abcd1234kdhfg")
)["Items"]

#print(res)
for item in res:
  print(
      f'{item.item["sk"]} - {item.item["typeNameAndDateSearch"]} - {item.item["dateTime"]}')
  #item.delete()
