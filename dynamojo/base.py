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


  __joined_sources__: list = PrivateAttr()
  __index_aliases__: dict = PrivateAttr()
  __index_keys__: list = PrivateAttr()

  __reserved__attributes__: ClassVar = [
    "__joined_sources__",
    "__index_aliases__",
    "__index_keys__"
  ]


  def __init__(
    self,
    **kwargs: dict
  ) -> None:


    for attribute in kwargs.items():
        if attribute in self.__reserved__attributes__:
            raise AttributeError(f"Attribute {attribute} is reserved")

    super().__init__(**kwargs)

    # Real key name -> alias name
    self.__index_aliases__ = {}
    # All sk and pk attributes
    self.__index_keys__ = []
    # List of attributes that are part of joins
    self.__joined_sources__ = []


    # Dict of `index key: alias name`
    for index_map in self._config.index_maps:
      if sk_att := index_map.sortkey:
        self.__index_aliases__[index_map.index.sortkey] = sk_att
      if getattr(index_map, "partitionkey", None):
        self.__index_aliases__[index_map.index.partitionkey] = index_map.partitionkey

    # All real index keys
    self.__index_keys__ = list(set(self.__index_aliases__.values()))


    for alias, sources in self._config.joined_attributes.items():
        self.__joined_sources__ += sources
        self.__fields__[alias] = ModelField.infer(
            name=alias,
            value=None,
            annotation=str,
            class_validators=None,
            config=self.__config__
        )
        self.__annotations__[alias] = str

    for attribute in self.dict():
        if attribute in self.__joined_sources__:
            self.set_compound_attribute(attribute)


    for index, alias in self.__index_aliases__.items():
        if not hasattr(self, alias):
            raise AttributeError(f"Cannot map Index attribute {index} to nonexistent attribute {alias}")

        self.__fields__[index] = self.__fields__[alias]
        self.__annotations__[index] = self.__annotations__[alias]
        super().__setattr__(index, self.__getattribute__(alias))


    for k, v in kwargs.items():
      if k in self._config.mutators:
        kwargs[k] = self.mutate_attribute(k, v)


  @property
  def item(self):
    return self.dict()


  def mutate_attribute(cls, field, val):
    return super().__setattr__(
      field,
      cls._config.mutators[field].callable(
        field,
        val,
        cls
      )
    )

  def __setattr__(cls, field, val, static_override=False):
    if field in cls.__reserved__attributes__:
        return super().__setattr__(field, val)

    if field in cls._config.mutators:
      val = cls.mutate_attribute(field, val)

    if (
      static_override is False
      and field in cls._config.static_attributes
      and hasattr(cls, field)
      and cls.__getattribute__(field) != val
    ):
      raise StaticAttributeError(f"Attribute '{field}' is immutable.")

    if field in cls.__index_keys__:
        for index, alias in cls.__index_aliases__.items():
            if field == alias:
                super().__setattr__(index, val)

    if field in cls.__joined_sources__:
        cls.set_compound_attribute(field)


    return super().__setattr__(field, val)


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
        cls.__setattr__(target, val)


  def save(self):
    """
    Stores our item in Dynamodb
    """

    return self._config.table.put_item(Item=self.dict())


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
