#!/usr/bin/env python3.8
from abc import (
  ABC,
  abstractproperty,
  abstractclassmethod
)
from copy import deepcopy
from collections import UserDict
from logging import getLogger
from typing import (
  Any,
  Callable,
  ClassVar,
  Dict,
  List,
  Optional,
  Union,
  TYPE_CHECKING
)

from boto3.session import Session
from boto3.dynamodb.conditions import (
  AttributeBase,
  ConditionBase,
  Key
)
from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel, Field, PrivateAttr
from pydantic.fields import ModelField
from .index import (
  Index,
  IndexList,
  IndexMap,
  TableIndex
)
from .config import DynamojoConfig
from .exceptions import (
  ProtectedAttributeError,
  RequiredAttributeError,
  StaticAttributeError,
  UnknownAttributeError,
  IndexNotFoundError
)

if not TYPE_CHECKING:
  Table = object


class DynamojoBase(BaseModel):

  _config: DynamojoConfig = PrivateAttr()

  __reserved_attributes__: ClassVar = [
      "__joined_values__",
      "__index_aliases__",
      "__index_values__",
      "__index_keys__"
  ]


  __joined_values__: dict = PrivateAttr()
  __index_aliases__: dict = PrivateAttr()
  __index_values__: dict = PrivateAttr()
  __index_keys__: list = PrivateAttr()


  def __init__(
    self,
    **kwargs: dict
  ) -> None:

    self.__joined_values__ = {}
    self.__index_aliases__ = {}
    self.__index_values__ = {}
    self.__index_keys__ = []

    args = deepcopy(kwargs)
    for index in self._config.index_maps:
        if pk := getattr(index.index, "partitionkey", None):
            self.__index_keys__.append(pk)
        if sk := getattr(index.index, "sortkey", None):
            self.__index_keys__.append(sk)

    for attribute in args:
      if (
        attribute in self._config.joined_attributes
        or attribute in self.__index_keys__
      ):
        del kwargs[attribute]

    super().__init__(**kwargs)

    for k, v in kwargs.items():
      if k in self._config.mutators:
        kwargs[k] = self.mutate_attribute(k, v)


    for attribute in self.dict():
      if attribute not in kwargs:
        self.__delattr__(attribute)


    for index_map in self._config.index_maps:
      if sk_att := index_map.sortkey:
        self.__index_aliases__[index_map.index.sortkey] = sk_att
      if getattr(index_map, "partitionkey", None):
        self.__index_aliases__[index_map.index.partitionkey] = index_map.partitionkey


    for item in self.item:
      self.set_compound_attribute(item)

    self.set_index_values()


  def mutate_attribute(cls, field, val):
    return super().__setattr__(
      field,
      cls._config.mutators[field].callable(
        field,
        val,
        cls
      )
    )

  @property
  def item(self):
    return {
      **super().__dict__,
      **self.__index_values__,
      **self.__joined_values__
    }

  def __getattr__(self, field):
    if field in self.item:
      return self.item[field]
    else:
      return super().__getattribute__(field)

  def __setattr__(cls, field, val, static_override=False):
    if field in cls.__reserved_attributes__:
        return super().__setattr__(field, val)

    if (
      static_override is False
      and field in cls._config.static_attributes
      and hasattr(cls, field)
      and cls.__getattribute__(field) != val
    ):
      raise StaticAttributeError(f"Attribute '{field}' is immutable.")

    for index, attribute_name in cls.__index_aliases__.items():
      if attribute_name == field:
        cls.__index_values__[index] = val

    cls.set_compound_attribute(field)

    if field in cls._config.mutators:
      val = cls.mutate_attribute(field, val)

    cls.set_index_values()

    return super().__setattr__(field, val)


  def set_index_values(self):
    for index, attribute_name in self.__index_aliases__.items():
      if hasattr(self, attribute_name):
        self.__index_values__[index] = self.__getattr__(attribute_name)

      if attribute_name in self._config.joined_attributes:
        self.__index_values__[index] = self.__joined_values__[attribute_name]


  @classmethod
  def fetch(cls, pk: str, sk: str = None) -> UserDict:
    """
    Returns an item from the database
    """

    key={
      cls._config.indexes.table.partitionkey: pk
    }

    if cls._config.indexes.table.sortkey:
      key[cls._config.indexes.table.sortkey] = sk

    item = cls._config.table.get_item(
      Key=key
    ).get("Item")

    if item:
      return cls(from_db=True, **item)


  def set_compound_attribute(cls, name):

    for target, attributes in cls._config.joined_attributes.items():
      if name in attributes:
        val = cls._config.join_separator.join([
          str(getattr(cls, attribute, "")) for attribute in attributes
        ])
        cls.__joined_values__[target] = val


  def save(self):
    """
    Stores our item in Dynamodb
    """

    return self._config.table.put_item(Item=self.item)


  @classmethod
  def query(
    cls,
    condition: ConditionBase,
    index: Index = None,
    filter: AttributeBase = None,
    limit: int = 1000,
    paginate: bool = False,
    start_key: dict = None
  ):
    """
    Runs a Dynamodb query using a condition from db.Index
    """


    items = []

    opts = {
      "Limit": limit,
      "KeyConditionExpression": condition
    }

    if index is None:
      index, condition = cls.get_index(condition)

    getLogger().info(f"Querying with index `{index.name}`")

    if start_key:
      opts["ExclusiveStartKey"] = start_key

    if filter:
      opts["FilterExpression"] = filter

    if not index.table_index:
      opts["IndexName"] = index.name

    while True:
      res = cls._config.table.query(**opts)
      items += res["Items"]

      if start_key := res.get("LastEvaluatedKey") and paginate is False:
        opts["ExclusiveStartKey"] = start_key
      else:
        break

    objects = []

    for item in res["Items"]:
        objects.append(cls(**item))

    res["Items"] = objects

    return res


  def delete(self):
    """
    Deletes an item from the table
    """
    key={
      self._config.indexes.table.partitionkey: self.__index_values__[self._config.indexes.table.partitionkey]
    }

    if self._config.indexes.table.is_composit:
      key[self._config.indexes.table.sortkey] = self.__index_values__[self._config.indexes.table.sortkey]

    res = self._config.table.delete_item(Key=key)

    return res


  @classmethod
  def get_index(cls, exp: ConditionBase, index: Index = None):

    def match_pk(alias):
      if index:
        return [index]

      matches = [
        mapper.index for mapper in cls._config.index_maps
        if hasattr(mapper, "partitionkey") and mapper.partitionkey == alias
      ]

      if not matches:
        raise IndexNotFoundError("No matching index found")

      return matches

    def match_sk(alias):
      if index:
        return [index]

      matches = [
        mapper.index for mapper in cls._config.index_maps
        if mapper.sortkey == alias
      ]

      if not matches:
        raise IndexNotFoundError("No matching index found")

      return matches

    # Get the aliases being used in the condition keys
    split_exp = list(exp._values)

    # There is only a pk operator, no sk if it is an instance of Key
    if isinstance(split_exp[0], Key):
      pk_alias = split_exp[0].name
      sk_alias = None
    else:
      pk_alias = split_exp[0]._values[0].name
      sk_alias = split_exp[1]._values[0].name



    pk_matches = match_pk(pk_alias)

    if sk_alias is None:
      # Return the table index if there are multiple matches
      for possibility in pk_matches:
        if isinstance(index, TableIndex):
          index = possibility
          break

      # Otherwise return the first match since it doesn't matter anyway
      if index is None:
        index = pk_matches[0]

      exp._values[0].name = index.partitionkey

      return index, exp

    # Find an index map that uses both keys
    sk_matches = match_sk(sk_alias)

    index_matches = [
      index for index in cls._config.indexes.values()
      if index in sk_matches and index in pk_matches
    ]

    if not index_matches:
      raise IndexNotFoundError("No matching index found")

    # Prefer Table index
    for possibility in cls.indexes.values():
      if isinstance(possibility, TableIndex):
        index = possibility
        break
      else:
        index = index_matches[0]


    exp._values[0]._values[0].name = index.partitionkey
    exp._values[1]._values[0].name = index.sortkey

    return index, exp
