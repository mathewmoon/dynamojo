#!/usr/bin/env python3.8
from datetime import datetime
from json import dumps
from os import environ
from typing import List
import requests

from dynamojo.index import IndexMap, get_indexes
from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from boto3.dynamodb.conditions import Key


TABLE = "test-dynamojo"
indexes = get_indexes("test-dynamojo")
DD_API_KEY = environ.get("DD_API_KEY")
DD_APP_KEY = environ.get("DD_APP_KEY")


class FooBase(DynamojoBase):
    accountId: str
    dateTime: str
    notificationType: str
    notificationName: str
    severity: int

    def save(self):
        self.dd_log()
        super().save()

    def sev_level(self, readable: bool = False):
        readable_map = {
            1: "info",
            2: "warning",
            3: "warning",
            4: "error",
            5: "critical"
        }

        sev = self.item().get("severity", 2)

        return readable_map[sev] if readable else sev

 
    def dd_log(self, log_level: str = None):
        url = " https://http-intake.logs.datadoghq.com/api/v2/logs"

        headers = {
            "DD-API-KEY": DD_API_KEY,
            "DD-APP-KEY": DD_APP_KEY
        }

        message = self.item().get("data", self.item())
        if not isinstance(message, dict):
            message = {
                "log_level": log_level or self.sev_level(readable=True),
                "message": message
            }
        else:
            message["log_level"] = log_level or self.sev_level(readable=True)

        params = {
            "ddsource": "AWSNotifications",
            "service": self.accountId,
            "message": dumps(message),
            "ddtags": ",".join([
                f"account:{self.accountId}",
                f"tenant:{self.item().get('tenant', '')}"
            ])
        }
        requests.post(url, json=params, headers=headers)


class MyFoo(FooBase):
    child_field: str
    second_child_field: str

    _config = DynamojoConfig(
        indexes=indexes,
        index_maps=[
            IndexMap(index=indexes.table, sortkey="dateTime",
                     partitionkey="accountId"),
            IndexMap(
                index=indexes.gsi0, sortkey="dateTime", partitionkey="notificationType"
            ),
            IndexMap(index=indexes.lsi0, sortkey="dateTime"),
        ],
        table=TABLE,
        joined_attributes={
            "dateTypeAndNameSearch": [
                "notificationType",
                "notificationName",
                "dateTime"
            ]
        },
        static_attributes=["dateTime", "accountId"],
        mutators=[]  # Mutator(source="dateTime", callable=mutate_sk)
    )


dt = datetime.now().isoformat()

foo = MyFoo(
    accountId="abcd1234kdhfg",
    dateTime=dt,
    notificationName="TestName",
    notificationType="ALARM",
    child_field="child",
    second_child_field="second child",
    severity=5
)
foo.save()
foo = MyFoo.fetch("abcd1234kdhfg", dt)
condition = Key("accountId").eq("abcd1234kdhfg") & Key("dateTime").eq(dt)
res = MyFoo.query(KeyConditionExpression=condition)
print(res["Items"][0].item())
