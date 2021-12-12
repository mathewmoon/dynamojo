#!/usr/bin/env python3
from collections import UserDict
from typing import List

from boto3 import client
from boto3.dynamodb.conditions import (
  ConditionBase,
  Key
)

from json import dumps

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

        self.__partitionkey = Key(partitionkey) if partitionkey else None
        self.__sortkey = Key(sortkey) if sortkey else None
        self.__name = name
        self.__partitionkey_name = partitionkey
        self.__sortkey_name = sortkey
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
    def partitionkey_name(self) -> str:
        """ Attribute name of the partition key"""
        return self.__partitionkey_name
    
    @property
    def sortkey_name(self) -> str:
        """ Attribute name of the sort key"""
        return self.__sortkey_name

    @property
    def name(self) -> str:
        """ Name of the index """
        return self.__name

    def _add_expression_attributes(self, expression: ConditionBase) -> ConditionBase:
      expression.name = self.name
      expression.table_index = self.table_index
      return expression

    def eq(self, pk, sk=None) -> ConditionBase:
      if sk and not self.sortkey:
        raise ValueError(f"Index {self.name} does not contain a sortkey")

      expression = self.partitionkey.eq(pk)
      if sk:
          expression = expression & self.sortkey.eq(sk)
      return self._add_expression_attributes(expression)

    def begins_with(self, pk, sk) -> ConditionBase:
        if not self.sortkey:
          raise ValueError(f"Index {self.name} does not contain a sortkey")
 
        expression = self.partitionkey.eq(pk) & self.sortkey.begins_with(sk)
        return self._add_expression_attributes(expression)

    def between(self, pk, start, end) -> ConditionBase:
        if not self.__sortkey:
          raise ValueError(f"Index {self.name} does not contain a sortkey")

        expression = self.partitionkey.eq(
            pk) & self.sortkey.between(start, end)
        return self._add_expression_attributes(expression)

    def gt(self, pk, sk) -> ConditionBase:
        if not self.__sortkey:
          raise ValueError(f"Index {self.name} does not contain a sortkey")

        expression = self.partitionkey.eq(pk) & self.sortkey.gt(sk)
        return self._add_expression_attributes(expression)

    def gte(self, pk, sk) -> ConditionBase:
        if not self.__sortkey:
          raise ValueError(f"Index {self.name} does not contain a sortkey")

        expression = self.partitionkey.eq(pk) & self.sortkey.gte(sk)
        return self._add_expression_attributes(expression)

    def lt(self, pk, sk) -> ConditionBase:
        if not self.__sortkey:
          raise ValueError(f"Index {self.name} does not contain a sortkey")

        expression = self.partitionkey.eq(pk) & self.sortkey.lt(sk)
        return self._add_expression_attributes(expression)

    def lte(self, pk, sk) -> ConditionBase:
        if not self.__sortkey:
          raise ValueError(f"Index {self.name} does not contain a sortkey")

        expression = self.partitionkey.eq(pk) & self.sortkey.lte(sk)
        return self._add_expression_attributes(expression)


class Gsi(Index):
    def __init__(
        self,
        name: str,
        sortkey: str,
        partitionkey: str = None
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
    data = {}
    def __init__(self, *args: List[Index]) -> None:
        has_table = False
        super().__init__()
        for index in args:
            if not isinstance(index, Index):
                raise TypeError("Invalid type for Index")
            if isinstance(index, TableIndex):
                if has_table:
                    raise ValueError("An IndexList object can only have one TableIndex")
                has_table = True

            self[index.name] = index

    def __setitem__(self, key, value):
        super().__setattr__(key, value)
        return super().__setitem__(key, value)


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
                if attr["KeyType"] == "HASH":
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
                "Lsi indexes cannot map to the partition key. Use index with type TableMap to map the partition key"
            )
        
        if index.partitionkey and not partitionkey:
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