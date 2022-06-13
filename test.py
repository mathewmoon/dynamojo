
#!/usr/bin/env python3
from collections import UserDict
from os import environ
from typing import List

from boto3 import resource
from mypy_boto3_dynamodb.service_resource import Table


from dynamojo.db import IndexMap, get_indexes, IndexList
from dynamojo.base import ObjectBase
from boto3.dynamodb.conditions import Key


###
#
# This is an example class
#
##
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


###
#  Using the class we just made
###
foo = Foo(
  notificationType="TEST_ALARM_TYPE",
  notificationName="notification name test",
  dateTime="3456778554363456",
  accountId="MYACCOUNT_12345",
  message="Test message"
)
foo.save()

# Parent class will automatically detect that we are using attributes mapped to the table index
condition = Key("accountId").eq("MYACCOUNT_12345") & Key("typeNameAndDateSearch").begins_with("TEST_ALARM_TYPE")
res = Foo.query(condition)
print(res)

# Parent class will automatically detect we are using gsi0
condition = Key("notificationType").eq("TEST_ALARM_TYPE") & Key("dateTime").gt("0")
res = Foo.query(condition)
print(res)


###
#  This library is very opinionated about how the table's indexes should be structured. Below is Terraform that shows the
#  correct way to set up the table. Index keys are never referenced directly when using the table. Rely on IndexMap for that.
#  Since LSI's can only be created at table creation time, and all indexes cost nothing if not used, we go ahead and create
#  all of the indexes that AWS will allow us to when the table is created.
##

"""
resource "aws_dynamodb_table" "test_table" {
  name         = "test-dynamojo"
  hash_key     = "pk"
  range_key    = "sk"
  billing_mode = "PAY_PER_REQUEST"

  # LSI attributes
  dynamic "attribute" {
    for_each = range(5)

    content {
      name = "lsi${attribute.value}_sk"
      type = "S"
    }
  }

  # GSI pk attributes
  dynamic "attribute" {
    for_each = range(20)

    content {
      name = "gsi${attribute.value}_pk"
      type = "S"
    }
  }

  # GSI sk attributes
  dynamic "attribute" {
    for_each = range(20)

    content {
      name = "gsi${attribute.value}_sk"
      type = "S"
    }
  }

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # GSI's
  dynamic "global_secondary_index" {
    for_each = range(20)

    content {
      name            = "gsi${global_secondary_index.value}"
      hash_key        = "gsi${global_secondary_index.value}_pk"
      range_key       = "gsi${global_secondary_index.value}_sk"
      projection_type = "ALL"
    }
  }

  # LSI's
  dynamic "local_secondary_index" {
    for_each = range(5)

    content {
      name            = "lsi${local_secondary_index.value}"
      range_key       = "lsi${local_secondary_index.value}_sk"
      projection_type = "ALL"
    }
  }
}

"""
