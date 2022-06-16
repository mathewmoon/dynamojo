#!/usr/bin/env python3.8
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


from dynamojo.index import IndexMap, get_indexes, Index, Mutator
from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from boto3.dynamodb.conditions import Key

if not TYPE_CHECKING:
  Table = object

TABLE = resource("dynamodb").Table("test-dynamojo")
indexes = get_indexes("test-dynamojo")

#def mutate_sk(_, value, obj):
#  return "~".join([obj.notificationType, value])


class Foo(DynamojoBase):
  accountId: str
  dateTime: str
  notificationType: str
  notificationName: str

  _config = DynamojoConfig(
    indexes = indexes,
    index_maps = [
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
    ],
    table = TABLE,
    joined_attributes = {
      "typeNameAndDateSearch": [
        "notificationType",
        "notificationName",
        "dateTime"
      ]
    },
    static_attributes = ["dateTime", "accountId"],
    mutators = []    #  Mutator(source="dateTime", callable=mutate_sk)
  )

dt = datetime.now().isoformat()

#for _ in range(100):
#foo =  Foo(
#    accountId="abcd1234kdhfg",
#    dateTime=datetime.now().isoformat(),
#    notificationName="TestName",
#    notificationType="ALARM"
#)
#print(foo.save())
#exit()
#

res = Foo.query(
    Key("accountId").eq("abcd1234kdhfg")
)["Items"]

#print(res[0].item)
#print(res[1].item)

#print(res)
for item in res:
#  print(item.item)
  print(
    f'{item.item["sk"]} - {item.item["typeNameAndDateSearch"]} - {item.item["dateTime"]}')
  item.delete()
