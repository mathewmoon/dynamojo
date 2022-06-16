#!/usr/bin/env python3.8
from collections import UserDict
from typing import List, Callable, Any

from boto3 import client
from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel


class Mutator(BaseModel):
  source: str
  callable: Callable[[str, Any, object], Any]

  class Config:
    frozen = True
    arbitrary_types_allowed: True


class Index:
    """
    Used as a factory for creating objects that can generate KeyConditionExpressions and abstract the
    partion/sort key names from the developer using them.
    """

    def __init__(
        self,
        *,
        name: str,
        sortkey: str,
        partitionkey: str = None,
    ):

        self.__partitionkey = partitionkey if partitionkey else None
        self.__sortkey = sortkey if sortkey else None
        self.__name = name
        self.is_composit = partitionkey and sortkey

    @property
    def partitionkey(self) -> ConditionBase:
        """ The partition key of the index as Key(partition key name)"""
        return self.__partitionkey

    @property
    def sortkey(self) -> ConditionBase:
        """ The sort key of the index as Key(sort key name)"""
        return self.__sortkey

    @property
    def table_index(self) -> bool:
        """ Whether or not this index is the table index """
        return isinstance(self, TableIndex)

    @property
    def name(self) -> str:
        """ Name of the index """
        return self.__name


class Gsi(Index):
    def __init__(
        self,
        name: str,
        partitionkey: str,
        sortkey: str = None
    ):
        super().__init__(
            name=name,
            sortkey=sortkey,
            partitionkey=partitionkey
        )


class Lsi(Index):
    def __init__(
        self,
        name: str,
        sortkey: str
    ):
        super().__init__(
            name=name,
            sortkey=sortkey
        )


class TableIndex(Index):
    def __init__(
        self,
        name: str,
        partitionkey: str,
        sortkey: str = None
    ):
        super().__init__(
            name=name,
            partitionkey=partitionkey,
            sortkey=sortkey
        )


class IndexList(UserDict):
    def __init__(self, *args: List[Index]) -> None:
        super().__init__()
        has_table = False
        for index in args:
            if not isinstance(index, Index):
                raise TypeError("Invalid type for Index")
            if isinstance(index, TableIndex):
                if has_table:
                    raise ValueError("An IndexList object can only have one TableIndex")
                has_table = True

            super().__setattr__(index.name, index)
            self.data[index.name] = index


def get_indexes(table_name):
    CLIENT = client("dynamodb")
    desc = CLIENT.describe_table(TableName=table_name)["Table"]
    gsi_list = desc.get("GlobalSecondaryIndexes", [])
    lsi_list = desc.get("LocalSecondaryIndexes", [])

    table_list = [{
        "IndexName": "table",
        "KeySchema": desc["KeySchema"]
    }]


    def build_indexes(index_type, index_list):
        index_objects = []
        for index in index_list:
            args = {}
            args["name"] = index["IndexName"]

            for attr in index["KeySchema"]:
                if attr["KeyType"] == "HASH" and index_type != Lsi:
                    args["partitionkey"] = attr["AttributeName"]
                elif attr["KeyType"] == "RANGE":
                    args["sortkey"] = attr["AttributeName"]

            index_objects.append(index_type(**args))

        return index_objects

    indexes = IndexList(*[
        *build_indexes(Gsi, gsi_list),
        *build_indexes(Lsi, lsi_list),
        *build_indexes(TableIndex, table_list)
    ])
    return indexes


class IndexMap:
    index: Index
    pk: str
    sk: str = None

    def __init__(
        self,
        index: Index,
        partitionkey: str = None,
        sortkey: str = None
    ):
        if isinstance(index, Lsi) and partitionkey:
            raise ValueError(
                "Lsi indexes only specify a sort key and use the table's partition key"
            )

        elif index.partitionkey and not partitionkey:
            raise ValueError(f"Partition key required for index {index.name}")

        if index.sortkey and not sortkey:
            raise ValueError(f"Sort key required for index {index.name}")

        if sortkey and not index.sortkey:
            raise ValueError(f"Index {index.name} requires a sort key")

        if partitionkey:
            self.partitionkey = partitionkey

        if sortkey:
            self.sortkey = sortkey

        self.index = index

