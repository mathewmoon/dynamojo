#!/usr/bin/env python3
from abc import (
  ABC,
  abstractproperty,
  abstractclassmethod
)
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
from pydantic import BaseModel, Field
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

class Mutator(BaseModel):
  source: str
  callable: Callable[[str, Any, object], Any]

  class Config:
    frozen = True
    arbitrary_types_allowed: True


class DynamojoBase(BaseModel, ABC):
  class Config:
    arbitrary_types_allowed: True
    allow_mutation = True

  class Meta:
    compound_values: ClassVar[dict] = {}
    index_aliases: ClassVar[dict] = {}
    index_values: ClassVar[dict] = {}


  class DynamojoConfig:
    __set_by_user__ = False

    # A dictionary in the form of {"<target attribute>": ["source_att_one", "source_att_two"]} where <target_attribute> will
    # automatically be overwritten by the attributes of it's corresponding list being joined with '~'. This is useful for creating
    # keys that can be queried over using Key().begins_with() or Key().between() by creating a means to filter based on the compounded
    # attributes.
    joined_attributes: ClassVar[Dict[str, Union[List[str], Callable]]] = {}

    # A list of database `Index` objects from `dynamojo.indexes.get_indexes()``
    indexes: ClassVar[List[Index]] = []

    # A list of `IndexMap` objects used to map arbitrary fields into index attributes
    index_maps: ClassVar[List[IndexMap]] = []

    static_attributes: ClassVar[List[str]] = []

    # A Dynamodb table object
    table: ClassVar[Table] = None

    join_separator: str = "~"

    mutators = List[Mutator]


  @classmethod
  def load_config(cls):
    cls.Meta.compound_values = {}
    cls.Meta.index_aliases = {}
    cls.Meta.index_values = {}

    if getattr(cls.DynamojoConfig, "table", None) is None:
      raise AttributeError("`DynamojoConfig.table` must be set to a boto3 Table resource")
    
    default_values = {
      "joined_attributes": {},
      "index_maps": {},
      "static_attributes": [],
      "compound_separator": "~",
      "mutators": []
    }

    for k, v in default_values.items():
      if not hasattr(cls.DynamojoConfig, k) or getattr(cls.DynamojoConfig, k) is None:
        setattr(cls.DynamojoConfig, k, v)

    cls.DynamojoConfig._mutators = {
      mutator.source: mutator for mutator in cls.DynamojoConfig.mutators
    }


  def __init__(
    self,
    **kwargs: dict
  ) -> None:
    self.load_config()


    super().__init__(**kwargs)

    for k, v in kwargs.items():
      if k in self.DynamojoConfig._mutators:
        kwargs[k] = self.mutate_attribute(k, v)


    for attribute in self.dict():
      if attribute not in kwargs:
        self.__delattr__(attribute)

    if not bool([
        x for x in self.DynamojoConfig.index_maps
        if isinstance(x.index, TableIndex)
      ]):
      raise AttributeError(
        "INDEX_MAP must both have an index of type TableIndex"
      )


    for index_item in self.DynamojoConfig.index_maps:
      if sk_att := index_item.sortkey:
        self.Meta.index_aliases[index_item.index.sortkey] = sk_att
      if getattr(index_item, "partitionkey", None):
        self.Meta.index_aliases[index_item.index.partitionkey] = index_item.partitionkey

    for item in self.item:
      self.set_compound_attribute(item)

    self.set_index_values()


  def mutate_attribute(cls, field, val):
    return super().__setattr__(
      field,
      cls.DynamojoConfig._mutators[field].callable(
        field,
        val,
        cls
      )
    )

  @property
  def item(self):
    return {
      **self.dict(),
      **self.Meta.index_values,
      **self.Meta.compound_values
    }

  def __getattr__(self, field):
    if field in self.item:
      return self.item[field]
    else:
      return super().__getattribute__(field)

  def __setattr__(cls, field, val, static_override=False):
    if (
      static_override is False
      and field in cls.DynamojoConfig.static_attributes
      and hasattr(cls, field)
      and cls.__getattribute__(field) != val
    ):
      raise StaticAttributeError(f"Attribute '{field}' is immutable.")
  
    for index, attribute_name in cls.Meta.index_aliases.items():
      if attribute_name == field:
        cls.Meta.index_values[index] = val

    cls.set_compound_attribute(field)

    if field in cls.DynamojoConfig._mutators:
      val = cls.mutate_attribute(field, val)

    cls.set_index_values()

    return super().__setattr__(field, val)
 

  def set_index_values(self):
    for index, attribute_name in self.Meta.index_aliases.items():
      if hasattr(self, attribute_name):
        self.Meta.index_values[index] = self.__getattr__(attribute_name)

      if attribute_name in self.DynamojoConfig.joined_attributes:
        self.Meta.index_values[index] = self.Meta.compound_values[attribute_name]


  @classmethod
  def fetch(cls, pk: str, sk: str = None) -> UserDict:
    """
    Returns an item from the database
    """
    cls.load_config()

    key={
      cls.DynamojoConfig.indexes.table.partitionkey: pk
    }

    if cls.DynamojoConfig.indexes.table.sortkey:
      key[cls.DynamojoConfig.indexes.table.sortkey] = sk

    item = cls.DynamojoConfig.table.get_item(
      Key=key
    ).get("Item")
 
    if item:
      return cls(from_db=True, **item)


  def set_compound_attribute(cls, name):
    cls.load_config()

    for target, attributes in cls.DynamojoConfig.joined_attributes.items():
      if name in attributes:
        val = cls.DynamojoConfig.compound_separator.join([
          str(getattr(cls, attribute, "")) for attribute in attributes
        ])
        cls.Meta.compound_values[target] = val


  def save(self):
    """
    Stores our item in Dynamodb
    """

    return self.DynamojoConfig.table.put_item(Item=self.item)


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
    cls.load_config()

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
      res = cls.DynamojoConfig.table.query(**opts)
      items += res["Items"]

      if start_key := res.get("LastEvaluatedKey") and paginate is False:
        opts["ExclusiveStartKey"] = start_key
      else:
        break

    objects = []
    for x in res["Items"]:
      item = cls(**x)
      print(item.item)
#      print(f"{item.item['sk']} - {item.item['dateTime']} - {item.item['typeNameAndDateSearch']}")
      objects.append(item)

    res["Items"] = objects
    return res
    print(x)
    print(y)
    res["Items"] = [
      cls(**item) for item in res["Items"]
    ]

    return res


  def delete(self):
    """
    Deletes an item from the table
    """
    key={
      self.DynamojoConfig.indexes.table.partitionkey: self.Meta.index_values[self.DynamojoConfig.indexes.table.partitionkey]
    }

    if self.DynamojoConfig.indexes.table.is_composit:
      key[self.DynamojoConfig.indexes.table.sortkey] = self.Meta.index_values[self.DynamojoConfig.indexes.table.sortkey]
 
    res = self.DynamojoConfig.table.delete_item(Key=key)

    return res


  @classmethod
  def get_index(cls, exp: ConditionBase, index: Index = None):
    cls.load_config()

    def match_pk(alias):
      if index:
        return [index]

      matches = [
        mapper.index for mapper in cls.DynamojoConfig.index_maps
        if hasattr(mapper, "partitionkey") and mapper.partitionkey == alias
      ]

      if not matches:
        raise IndexNotFoundError("No matching index found")

      return matches

    def match_sk(alias):
      if index:
        return [index]

      matches = [
        mapper.index for mapper in cls.index_map
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
      index for index in cls.indexes.values()
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
