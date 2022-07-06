#!/usr/bin/env python3.8
from datetime import datetime


from dynamojo.index import IndexMap, get_indexes
from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from boto3.dynamodb.conditions import Key


TABLE = "test-dynamojo"
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
        mutators=[]  # Mutator(source="dateTime", callable=mutate_sk)
    )


dt = datetime.now().isoformat()

foo = Foo(
    accountId="abcd1234kdhfg",
    dateTime=datetime.now().isoformat(),
    notificationName="TestName",
    notificationType="ALARM",
)


condition = Key("notificationType").eq("ALARM") & Key("dateTime").gt("0")
res = Foo.query(KeyConditionExpression=condition)
res = Foo.fetch("abcd1234kdhfg", "ALARM~TestName~2022-06-16T01:03:20.439051")
print(res)
