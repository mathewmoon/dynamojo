#!/usr/bin/env python3
from datetime import datetime
from json import dumps
from os import environ
import requests

from dynamojo.index import IndexMap, get_indexes
from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from boto3.dynamodb.conditions import Key, Attr


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

    def save(self, **kwargs):
        self.dd_log()
        super().save(**kwargs)

    def sev_level(self, readable: bool = False):
        readable_map = {
            1: "info",
            2: "warning",
            3: "warning",
            4: "error",
            5: "critical",
        }

        sev = self.item().get("severity", 2)

        return readable_map[sev] if readable else sev

    def dd_log(self, log_level: str = None):
        url = " https://http-intake.logs.datadoghq.com/api/v2/logs"

        headers = {"DD-API-KEY": DD_API_KEY, "DD-APP-KEY": DD_APP_KEY}

        message = self.item().get("data", self.item())
        if not isinstance(message, dict):
            message = {
                "log_level": log_level or self.sev_level(readable=True),
                "message": message,
            }
        else:
            message["log_level"] = log_level or self.sev_level(readable=True)

        params = {
            "ddsource": "AWSNotifications",
            "service": self.accountId,
            "message": dumps(message),
            "ddtags": ",".join(
                [f"account:{self.accountId}", f"tenant:{self.item().get('tenant', '')}"]
            ),
        }
        requests.post(url, json=params, headers=headers)


class MyFoo(FooBase):
    child_field: str
    second_child_field: str

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
            "dateTypeAndNameSearch": [
                "notificationType",
                "notificationName",
                "dateTime",
            ]
        },
        store_aliases=False,
        static_attributes=["dateTime", "accountId"],
        mutators=[],  # Mutator(source="dateTime", callable=mutate_sk)
    )


dt = datetime.now().isoformat()

# Making a foo
foo = MyFoo(
    accountId="abcd1234kdhfg",
    dateTime=dt,
    notificationName="TestName",
    notificationType="ALARM",
    child_field="child",
    second_child_field="second child",
    severity=5,
)
print(f"Made object foo: {foo}")
print(f"Full item: {foo.item()}")

# Fails because of the condition check
print("\n\nTrying to save with a condition check that will return False")
try:
    foo.save(ConditionExpression=Attr("foo").eq("bar"))
except Exception as e:
    print(e)

# But succeeds without it
foo.save()

# Let's do a get_item() operation. The first arg is always the partition key
# the second (optional if the table doesn't use a sortkey) argument is the sortkey
print("\n\nTrying MyFoo.fetch() to get the object we just created.")
foo = MyFoo.fetch("abcd1234kdhfg", dt)
print(f"Got it {foo.item()}")

# Now lets do a query to get back the same item. We can use a filter expression too
print(
    "\n\nRunning a query that will return the same item using MyFoo.query() with a condition and filter expression"
)
condition = Key("accountId").eq("abcd1234kdhfg") & Key("dateTime").eq(dt)
filter = Attr("notificationName").eq("TestName")

# Notice that we don't have to specify the index. Dynamojo will figure out what index to use.
# It will always prefer the table. If there are multiple suitable indexes other than the table index
# it will take the first one. You can however specify an index to use by passing IndexName as either a
# string or an Index() object.
res = MyFoo.query(KeyConditionExpression=condition, FilterExpression=filter)
print(f"""Returned an item from the query: {res.Items[0]}""")

# Now lets do one that filters out all results
print("\n\nNow we are going to add a FilterExpression that we know won't match")
filter = Attr("notificationName").eq("YoMamma")

res = MyFoo.query(KeyConditionExpression=condition, FilterExpression=filter)

# You can see that there are no results
print(f"""Query with filter returned {len(res.Items)} items""")
print(res)