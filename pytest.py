#!/usr/bin/env python3.8
from json import dumps
from pydantic import BaseModel, validator

from abc import ABC, abstractclassmethod, abstractproperty
from collections import UserDict
from datetime import datetime

from typing import Any, ClassVar, Dict, List, Optional
from os import environ
from typing import List
from typing import TYPE_CHECKING
from boto3 import resource
from mypy_boto3_dynamodb.service_resource import Table


from dynamojo.index import IndexMap, get_indexes, Index, Mutator
from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from boto3.dynamodb.conditions import Key, Attr, And, ConditionBase, BuiltConditionExpression, ConditionExpressionBuilder, AttributeBase

import boto3.dynamodb.conditions

if not TYPE_CHECKING:
    Table = object

TABLE = resource("dynamodb").Table("test-dynamojo")
indexes = get_indexes("test-dynamojo")


class Foo(DynamojoBase):
    accountId: str
    dateTime: str
    notificationType: str
    notificationName: str

    _config = DynamojoConfig(
        indexes=indexes,
        index_maps=[
            IndexMap(index=indexes.table, sortkey="dateTime", partitionkey="accountId"),
            IndexMap(
                index=indexes.gsi0, sortkey="dateTime", partitionkey="notificationType"
            ),
            IndexMap(index=indexes.lsi0, sortkey="dateTime"),
        ],
        table=TABLE,
        joined_attributes={
            "typeNameAndDateSearch": [
                "notificationType",
                "notificationName",
                "dateTime",
            ]
        },
        static_attributes=["dateTime", "accountId"],
        mutators=[],  #  Mutator(source="dateTime", callable=mutate_sk)
    )


dt = datetime.now().isoformat()

# for _ in range(100):
foo = Foo(
    accountId="abcd1234kdhfg",
    dateTime=datetime.now().isoformat(),
    notificationName="TestName",
    notificationType="ALARM",
)


condition = Key("notificationType").eq("ALARM") & Key("dateTime").gt("0")
#res = Foo.query(KeyConditionExpression=condition)["Items"]

#item = res[0]
res = Foo.query(KeyConditionExpression=condition)
res = Foo.fetch("abcd1234kdhfg", "ALARM~TestName~2022-06-16T01:03:20.439051")
print(res)
exit()

"""
#print(foo.save())
#exit()
#


print(res)
exit()
#print(res[0].item)
#print(res[1].item)

#print(res)
for item in res:
#  print(item.item)
  print(
    f'{item.item["sk"]} - {item.item["typeNameAndDateSearch"]} - {item.item["dateTime"]}')
  item.delete()
"""
# pk = Tenant
# sk = SlackChannel~account~name
class SlackChannel(DynamojoBase):

    _config = DynamojoConfig(
        indexes=indexes,
        index_maps=[
            IndexMap(index=indexes.table, sortkey="dateTime", partitionkey="accountId"),
            IndexMap(
                index=indexes.gsi0, sortkey="dateTime", partitionkey="notificationType"
            ),
            IndexMap(index=indexes.lsi0, sortkey="dateTime"),
        ],
        table=TABLE,
        joined_attributes={
            "typeNameAndDateSearch": [
                "notificationType",
                "notificationName",
                "dateTime",
            ]
        },
        static_attributes=["dateTime", "accountId"],
        mutators=[],  #  Mutator(source="dateTime", callable=mutate_sk)
    )

