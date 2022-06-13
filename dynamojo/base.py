#!/usr/bin/env python3
from collections import UserDict
from logging import getLogger
from typing import (
  Any,
  Dict,
  List
)

from boto3.session import Session
from mypy_boto3_dynamodb.service_resource import Table
from boto3.dynamodb.conditions import (
  AttributeBase,
  ConditionBase,
  Key
)

from .db import (
  Index,
  IndexList,
  IndexMap,
  TableIndex
)
from .exceptions import (
  ProtectedAttributeError,
  RequiredAttributeError,
  StaticAttributeError,
  UnknownAttributeError,
  IndexNotFoundError
)

from abc import ABC, abstractproperty

class ObjectBase(UserDict, ABC):
  """
  Provides a base class for objects that will be stored in Dynamodb. This class, when used with the db submodule
  allows for writing classes that require only the most basic of definition to have all the methods required to operate.
   * CRUD operations are provided by this class and can be overriden in subclasses.
   * The class is structured to allow for lazy loading of child resources if subclasses are created correctly
   * New object types represented as subclasses should not have to know anything about the underlying table structure. Indexes are managed
     by the db.Indexes class and setting YourClass.INDEX_MAP to control the mapping of attributes in your object to the pk/sk of a particular index
     on the fly
   * New classes should not ever have to make calles to Dynamodb. If they do then either the subclass is not using the base correctly or there is
     something that needs to be changed in the base class to accommodate.
  
  Creating a subclass:
    * Create a class that inherits from ObjectBase
    * Set a class attribute called INDEX_MAP that contains a list/tuple of IndexMap objects. This will map attributes on your object to index attributes
    * call super().__init__(**kwargs) in your __init__() method
    * If you need to add functionality to any methods in Object base, such as doing some custom logic before Object.save() then implement that method in
      your class and at the end call super().<method>()
      Example:

        ```
          def save(self):
            self.foo = "bar"
            return super().save()
        ```
    Auth:
      Before making any calls (class methods or object creation) make sure to set the caller and whether they are an admin
      Example:

        ```
        from db_types import set_caller

        caller = event["identity"]["claims"]["preferred_username"]
        admin = "AWS Admin" in event["identity"]["claims"]["groups"]
        set_caller(caller, is_admin=admin)

        foo = Bar.fetch("foo")
        ```
  """

  @abstractproperty
  def _table(self) -> Table:
    pass

  # Attributes that cannot be changed once set
  _static_attributes: List[str] = []

  # Attributes that will raise an exception on save if not set
  _required_attributes: List[str] = []

  # Attributes that are optional
  _optional_attributes: List[str] = []

  # Cannot be changed by instances directly. All attributes on indexes get pushed into this list
  __protected_attributes: List[str] = []

  # A dictionary in the form of {"<target attribute>": ["source_att_one", "source_att_two"]} where <target_attribute> will
  # automatically be overwritten by the attributes of it's corresponding list being joined with '~'. This is useful for creating
  # keys that can be queried over using Key().begins_with() or Key().between() by creating a means to filter based on the compounded
  # attributes.
  compound_properties: Dict[str, List[str]] = {}
  
  # A list of IndexObjects that represent indexes in the table
  @abstractproperty
  def indexes(self) -> IndexList:
    pass

  # All subclasses must implement this tuple/list of IndexMap items, which will cause mapped attributes to
  # be automatically duplicated into indexes. Must contain at minimum a IndexMap item for Indexes.table
  @abstractproperty
  def index_map(self) -> List[IndexMap]:
    pass

  data = {}
  _MAPPED_ATTRIBUTES = []
  __initialized = False

  def __init__(
    self,
    from_db: bool = False,
    **kwargs: dict
  ) -> None:

    super().__init__()

    self._INDEX_MAP = {}

    # Lets us know to allow setting protected attributes since they came from the db and not user input
    self.from_db = from_db

    self.TABLE_INDEX = [
      index for _, index in self.indexes.items()
      if isinstance(index, TableIndex)
    ][0]

    if not bool([
        x for x in self.index_map
        if isinstance(x.index, TableIndex)
      ]):
      raise AttributeError(
        "INDEX_MAP must both have an index of type TableIndex"
      )

    self.make_index_map()

    self.__protected_attributes += list(self._INDEX_MAP.keys())

    # After setting self.__initialized to True then any attributes we update will have their corresponding indexes updated
    self.__initialized = True


    self.__force_setattr(
      "objectType",
      self.__class__.__name__
    )

    self.__force_setattr(
      "__typename",
      self.__class__.__name__
    )

    self.update(kwargs)

  def make_index_map(self):
    """
    Make a slightly friendlier map to use for parsing attribute -> index key mappings.
    Each key is an index attribute name (gsi0_sk, pk, lsi1_sk, etc) and the value is the
    object's attribute that should be mapped to it.
    """
    new_map = {}

    for index_item in self.index_map:
      if sk_att := index_item.sortkey:
        new_map[index_item.index.sortkey] = sk_att
      if hasattr(index_item, "partitionkey") and index_item.partitionkey is not None:
        new_map[index_item.index.partitionkey] = index_item.partitionkey

    for attr in new_map.values():
      if isinstance(attr, str):
        self._required_attributes.append(attr)
      else:
        self._required_attributes += attr
 
    self._required_attributes += list(new_map.keys())
    self.__protected_attributes += list(new_map.keys())

    self._INDEX_MAP.update(new_map)

    self._MAPPED_ATTRIBUTES += list(new_map.values())
  
  @property
  def prefix(self) -> str:
    return f"{self.__class__.__name__}~"

  @property
  def _all_attributes(self) -> List[str]:
    return list(set([
      *self._static_attributes,
      *self._required_attributes,
      *self.__protected_attributes,
      *self._optional_attributes,
      "objectType",
      "__typename"
    ]))

  @property
  def protected_attributes(self) -> List[str]:
    return self.__protected_attributes

  @property
  def optional_attributes(self) -> List[str]:
    return self._optional_attributes

  @classmethod
  def fetch(cls, pk: str, sk: str = None) -> UserDict:
    """
    Returns an item from the database
    """
    key={
      cls.indexes.table.partitionkey: pk
    }

    if cls.indexes.table.sortkey:
      key[cls.indexes.table.sortkey] = sk

    item = cls._table.get_item(
      Key=key
    ).get("Item")
 
    if item:
      return cls(from_db=True, **item)

  def __setitem__(self, key: str, item: Any) -> None:
    """
    Ensures that __setitem__ and __setattr__ behave the same way.
    """
    return self.__setattr__(key, item)

  def __setattr__(self, name: str, value: Any) -> None:
    """
    Providers a setter that is aware of static/protected attributes, ensures that items are added to self.data
    and all indexes are updated.
    """
    if (
      name in self._static_attributes
      and name in self.data
      and value != self.data[name]
    ):
      raise StaticAttributeError(f"Attribute {name} is static and cannot be modified directly once set")

    if name in self.__protected_attributes and not self.from_db:
      raise ProtectedAttributeError(
          f"Attribute {name} is reserved and cannot be set directly")

    if name in [*self._required_attributes, *self._optional_attributes]:
      super().__setitem__(name, value)
    super().__setattr__(name, value)

    # Important that this comes last
    self.__update_indexes(name, value)

  def __update_indexes(self, name: str, value: Any) -> None:
    """
    Anytime an attribute that is mapped, via self.INDEX_MAP, is set this will be called and
    any corresponding index attributes that are mapped to the attribute being set will be updated
    """
    if not (
      self.__initialized
#      and name in self._MAPPED_ATTRIBUTES
    ):
      
      return

    if value is None:
      raise ValueError(f"Invalid value for '{name}'. Attributes mapped to indexes cannot be None")

    for index_key, attribute in self._INDEX_MAP.items():
      if name == attribute:
        self.__force_setattr(index_key, value, update_index=False)

    self.set_compound_attributes(name)
  #def __getattribute__(self, name: str) -> Any:
  #  try:
  #    return super().__getitem__(name)
  #  except (NameError, KeyError):
  #    return super().__getattribute__(name)

  def set_compound_attributes(self, name):
    for target, attributes in self.compound_attributes.items():
      if name in attributes:
        val = "~".join([
          self.get(attribute, "") for attribute in attributes
        ])
        self.__force_setattr(target, val)


  def validate_attributes(self) -> None:
    """
    Validates that required attributes are set and that there are no unknown attributes that
    have leaked into self.data
    """
    for att in self._required_attributes:
      if not self.get(att):
        raise RequiredAttributeError(f"Missing required attribute {att}")

    for att in self.data:
      if att not in self._all_attributes:
        raise UnknownAttributeError(
            f"Unknown attribute {att} for object of type {self.objectType}")

  def __force_setattr(self, name: Any, value: Any, update_index=True) -> None:
    """
    Helper that allows us to set attributes that are protected when calling self.__setattr__
    """
    if update_index:
      self.__update_indexes(name, value)
    super().__setitem__(name, value)
    return super().__setattr__(name, value)

  def save(self):
    """
    Stores our item in Dynamodb
    """
    self.validate_attributes()

    self._table.put_item(Item=self.data)

    return self

  @classmethod
  def get_prefix(cls) -> str:
    return f"{cls.__name__}~"

  @classmethod
  def list(
    cls,
    filter: AttributeBase = None
  ) -> UserDict:
    """
    Returns a list of all objects of a certain type, optionally filtered by `filter`
    """
    
    opts = {
      "index": cls.INDEXES.gsi0.eq(cls.__name__)
    }

    if filter:
      opts["filter"] = filter

    return cls.query(**opts)["Items"]
  
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
      res = cls._table.query(**opts)
      items += res["Items"]

      if start_key := res.get("LastEvaluatedKey") and paginate is False:
        opts["ExclusiveStartKey"] = start_key
      else:
        break

    res["Items"] = [
      cls(from_db=True, **item) for item in res["Items"]
    ]

    return res


  def delete(self):
    """
    Deletes an item from the table
    """
    key={
      self.INDEXES.table.partitionkey: self[self.INDEXES.table.partitionkey]
    }

    if self.INDEXES.table.is_composit:
      key[self.INDEXES.table.sortkey] = self[self.INDEXES.table.sortkey]

    self._table.delete_item(Key=key)

    return True


  @classmethod
  def get_index(cls, exp: ConditionBase, index: Index = None):

    def match_pk(alias):
      if index:
        return [index]

      matches = [
        mapper.index for mapper in cls.index_map
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
