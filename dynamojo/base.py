#!/usr/bin/env python3
from collections import UserDict
from os import environ
from typing import (
  Any,
  List
)

from boto3.session import Session
from boto3.dynamodb.conditions import (
  AttributeBase,
  ConditionBase
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
  UnknownAttributeError
)

class ObjectBase(UserDict):
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

  __TABLE = Session().resource("dynamodb").Table(environ.get("DYNAMODB_TABLE"))

  print(type(__TABLE))
  # Attributes that cannot be changed once set
  __static_attributes: List[str]  = []

  # Attributes that will raise an exception on save if not set
  __required_attributes: List[str] = []

  # Cannot be changed by instances directly. All attributes on indexes get pushed into this list
  __protected_attributes: List[str] = [
    "__typename"
  ]

  # A list of IndexObjects that represent indexes in the table
  INDEXES: IndexList
  
  # Internally used by UserDict
  data: dict = {}

  __INDEX_MAP = {}
  __MAPPED_ATTRIBUTES = []
  __initialized = False

  # All subclasses must implement this tuple/list of IndexMap items, which will cause mapped attributes to
  # be automatically duplicated into indexes. Must contain at minimum a IndexMap item for Indexes.table
  INDEX_MAP: List[IndexMap]

  def __init__(
    self,
    required_attributes: List[str],
    static_attributes: List[str],
    optional_attributes: List[str],
    from_db: bool = False,
    **kwargs: dict
  ) -> None:

    # Lets us know to allow setting protected attributes since they came from the db and not user input
    self.from_db = from_db

    if not hasattr(self, "INDEXES"):
      raise AttributeError("Classes must declare a list of indexes as `cls.INDEXES`")

    if not hasattr(self, "INDEX_MAP"):
      raise AttributeError("Classes must declare `cls.INDEX_MAP`")

    self.TABLE_INDEX = [
      index for _, index in self.INDEXES.items()
      if isinstance(index, TableIndex)
    ][0]

    if not bool([
        x for x in self.INDEX_MAP
        if isinstance(x.index, TableIndex)
      ]):
      raise AttributeError(
        "INDEX_MAP must both have an index of type TableIndex"
      )

    self.make_index_map()

    self.__protected_attributes += list(self.__INDEX_MAP.keys())
    self.__protected_attributes = list(set(self.__protected_attributes))   
    self.__optional_attributes = optional_attributes

    self.__required_attributes = list(set([
      *self.__required_attributes,
      *required_attributes
    ]))

    self.__static_attributes = list(set([
      *self.__static_attributes,
      *static_attributes
    ]))

    self.__all_attributes = list(set([
      *optional_attributes,
      *self.__static_attributes,
      *self.__required_attributes,
      *self.__protected_attributes
    ]))

    # After setting self.__initialized to True then any attributes we update will have their corresponding indexes updated
    self.__initialized = True

    super().__init__()

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

    for index_item in self.INDEX_MAP:
      if sk_att := index_item.sortkey:
        new_map[index_item.index.sortkey_name] = sk_att
      if pk_att := index_item.partitionkey:
        new_map[index_item.index.partitionkey_name] = pk_att

    for attr in new_map.values():
      if isinstance(attr, str):
        self.__required_attributes.append(attr)
      else:
        self.__required_attributes += attr
    self.__required_attributes += list(new_map.keys())
    self.__protected_attributes += list(new_map.keys())

    self.__INDEX_MAP.update(new_map)

    self.__MAPPED_ATTRIBUTES += list(new_map.values())

  @property
  def prefix(self) -> str:
    return f"{self.__class__.__name__}~"

  @property
  def required_attributes(self) -> List[str]:
    return self.__required_attributes
  
  @property
  def static_attributes(self) -> List[str]:
    return self.__static_attributes
  
  @property
  def all_attributes(self) -> List[str]:
    return self.__all_attributes

  @property
  def protected_attributes(self) -> List[str]:
    return self.__protected_attributes

  @property
  def optional_attributes(self) -> List[str]:
    return self.__optional_attributes

  @classmethod
  def fetch(cls, pk: str, sk: str = None) -> UserDict:
    """
    Returns an item from the database
    """

    key={
      cls.INDEXES.table.partitionkey_name: pk
    }

    if cls.INDEXES.table.sortkey:
      key[cls.INDEXES.table.sortkey_name] = sk

    item = cls.__TABLE.get_item(
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
      name in self.static_attributes
      and name in self.data
      and value != self.data[name]
    ):
      raise StaticAttributeError(f"Attribute {name} is static and cannot be modified directly once set")

    if name in self.protected_attributes and not self.from_db:
      raise ProtectedAttributeError(
          f"Attribute {name} is reserved and cannot be set directly")

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
      and name in self.__MAPPED_ATTRIBUTES
    ):
      return

    if value is None:
      raise ValueError(f"Invalid value for '{name}'. Attributes mapped to indexes cannot be None")

    for index_key, attribute in self.__INDEX_MAP.items():
      if (
        isinstance(attribute, str)
        and name == attribute
      ) or (
        isinstance(attribute, (list, tuple, set))
        and name in attribute
      ):
        if isinstance(attribute, (list, tuple, set)):
          attributes = attribute
          final_value = "~".join([
            self.get(attribute, "") for attribute in attributes
          ])
        else:
          final_value = value
        
        self.__force_setattr(index_key, final_value, update_index=False)

 
  def __getattribute__(self, name: str) -> Any:
    try:
      return super().__getattribute__(name)
    except NameError:
      return None

  def validate_attributes(self) -> None:
    """
    Validates that required attributes are set and that there are no unknown attributes that
    have leaked into self.data
    """
    for att in self.required_attributes:
      if not self.get(att):
        raise RequiredAttributeError(f"Missing required attribute {att}")

    for att in self.data:
      if att not in self.all_attributes:
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

    self.__TABLE.put_item(Item=self.data)

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
    index: ConditionBase,
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
      "KeyConditionExpression": index
    }

    if start_key:
      opts["ExclusiveStartKey"] = start_key

    if filter:
      opts["FilterExpression"] = filter

    if not index.table_index:
      opts["IndexName"] = index.name

    while True:
      res = cls.__TABLE.query(**opts)
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
      self.INDEXES.table.partitionkey_name: self[self.INDEXES.table.partitionkey_name]
    }

    if self.INDEXES.table.is_composit:
      key[self.INDEXES.table.sortkey_name] = self[self.INDEXES.table.sortkey_name]

    self.__TABLE.delete_item(Key=key)

    return True
