#!/usr/bin/env python3
from __future__ import annotations
from collections import UserDict
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Dict, List, TypeVar, Union

from boto3.dynamodb.conditions import (
    AttributeBase,
    ConditionBase,
    ConditionExpressionBuilder,
)
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from pydantic import BaseModel, PrivateAttr
from .boto import DYNAMOCLIENT
from .index import Index, Lsi
from .config import DynamojoConfig
from .exceptions import StaticAttributeError, IndexNotFoundError


class DynamojoBase(BaseModel):
    """A class to use as a base for modeling objects to store in Dynamodb. This class
    is intended to be inherited by another class, which actually defines the model.
    """    

    #: All subclasses should override with their own config of the type `:class:dynamojo.config.DynamojoConfig`
    _config: DynamojoConfig = PrivateAttr()

    def __new__(cls: DynamojoModel, *_: List[Any], **__: Dict[Any, Any]) -> DynamojoModel:
        if cls is DynamojoBase:
            raise TypeError(f"{cls} must be subclassed")
        return super().__new__(cls)

    def __init__(self: DynamojoModel, **kwargs: Dict[str, Any]) -> None:

        """Initialize a new object with any required or optional attributes"""        
        super().__init__(**kwargs)

        for attr in self._config.joined_attributes:
            if attr in self.dict():
                raise AttributeError(
                    f"Attribute '{attr}' cannot be declared as a member and a joined field"
                )
        for attr in self._config.__index_keys__:
            if attr in self.dict():
                raise AttributeError(
                    f"Attribute {attr} is part of an index and cannot be declared directly. Use IndexMap() and an alias instead"
                )

        # TODO: Flush out these mutators.
        for k, v in kwargs.items():
            if k in self._config.mutators:
                kwargs[k] = self._mutate_attribute(k, v)

    def __getattribute__(self: DynamojoModel, name: str) -> Any:
        if super().__getattribute__("_config").joined_attributes.get(name):
            return self._generate_joined_attribute(name)

        return super().__getattribute__(name)

    def __setattr__(self: DynamojoModel, field: str, val: Any) -> None:
        # Mutations should happen first to allow for other dynamic fields to get their updated value
        if field in self._config.mutators:
            val = self._mutate_attribute(field, val)

        if field in self._config.joined_attributes:
            raise AttributeError(
                f"Attribute '{field}' is a joined field and cannot be set directly"
            )

        if field in self._config.__index_keys__:
            raise AttributeError(
                f"Attribute '{field}' is an index key and cannot be set directly. Use IndexMap() to create an alias"
            )

        # Static fields can only be set once
        if (
            field in self._config.static_attributes
            and hasattr(self, DynamojoModel, field)
            and self.__getattribute__(field) != val
        ):
            raise StaticAttributeError(f"Attribute '{field}' is immutable.")

        return super().__setattr__(field, val)

    def _db_item(self) -> Dict[str, Any]:
        """
        Returns the item exactly as it is in the database. If self._config.store_aliases is False
        then those aliases will be omitted.
        """
        item = {**self.dict(), **self.joined_attributes(), **self.index_attributes()}
        if not self._config.store_aliases:
            for attr in self._config.__index_aliases__.values():
                if attr in item:
                    del item[attr]
        return item

    def _generate_joined_attribute(self: DynamojoModel, name: str) -> str:
        """
        Takes attribute names defined in self._config.joined_attributes and stores them with the values
        of the corresponding attributes concatenated by self._config.join_separator.
        """
        item = super().__getattribute__("dict")()
        sources = super().__getattribute__("_config").joined_attributes.get(name)
        new_val = [item.get(source, "") for source in sources]
        return self._config.join_separator.join(new_val)

    @classmethod
    def _get_index_from_attributes(
        cls, partitionkey: str = None, sortkey: str = None
    ) -> Index:
        """
        Returns an Index() object based on attributes being passed as arguments.
        If multiple indexes match then the table index, if matched is returned first. If
        not the table index then the first match in the list.
        """
        for x in cls._config.index_maps:
            if x.index.name == "table":
                table_index_map = x
                break
        matches = {}

        for mapping in cls._config.index_maps:

            if isinstance(mapping.index, Lsi):
                pk = table_index_map.partitionkey
            else:
                pk = mapping.partitionkey

            if hasattr(mapping, "sortkey"):
                sk = mapping.sortkey
            else:
                sk = None

            if (
                # If we only had one key specified it HAS to be the partition
                sortkey is None
                and partitionkey == pk
            ) or (
                partitionkey is not None
                and sortkey is not None
                and (
                    pk == partitionkey
                    # Catch cases where our key is not composite (yuck!)
                    and (sk == sortkey or sk is None)
                )
            ):
                matches[mapping.index.name] = mapping.index

        if not matches:
            raise IndexNotFoundError(
                "Could not find a suitable index. Either specify a valid index or change the Condition statement"
            )

        return matches.get("table", list(matches.values())[0])

    @classmethod
    def _get_raw_condition_expression(
        self,
        exp: ConditionBase,
        index: Union[Index, str] = None,
        expression_type="KeyConditionExpression",
    ):
        """
        Take a boto3.dynamodb.conditions.ConditionBase object and convert it to a dictionary suitable as the condition expression,
        expression attribute names, and expression attribute values for an operation using the low-level dynamodb client.
        This allows usage of boto3.dynamodb.conditions.Condition deconstruction to be used for the sake of automatically selecting
        indexes.  Placeholder prefixes are replaced so that attribute names and attribute values dicts can be merged together in queries
        where multiple types of condition expressions are passed.
        """
        is_key_condition = expression_type == "KeyConditionExpression"
        raw_exp = ConditionExpressionBuilder().build_expression(
            exp, is_key_condition=is_key_condition
        )

        if is_key_condition:
            attribute_names = list(raw_exp.attribute_name_placeholders.values())
            if len(attribute_names) == 1:
                attribute_names.append(None)

            if isinstance(index, str):
                index = self.get_index_by_name(index)

            if index is None:
                index = self._get_index_from_attributes(*attribute_names)

            for placeholder, attr in raw_exp.attribute_name_placeholders.items():
                if attr == attribute_names[0]:
                    raw_exp.attribute_name_placeholders[
                        placeholder
                    ] = index.partitionkey
                if len(attribute_names) == 2 and attr == attribute_names[1]:
                    raw_exp.attribute_name_placeholders[placeholder] = index.sortkey

        for k, v in raw_exp.attribute_value_placeholders.items():
            raw_exp.attribute_value_placeholders[k] = TypeSerializer().serialize(v)

        opts = {
            "ExpressionAttributeNames": {},
            "ExpressionAttributeValues": {},
            expression_type: raw_exp.condition_expression,
        }

        # We have to do the dance below to keep queries that use KeyConditionExpression
        # and FilterExpressionfrom having their name/value placeholders clobber each other
        # when the dicts are merged to for the full query args
        original_name_prefix = "#n"
        original_value_prefix = ":v"

        if expression_type == "KeyConditionExpression":
            new_name_prefix = "#key_name"
            new_value_prefix = ":key_value"
            if index.name != "table":
                opts["IndexName"] = index.name

        elif expression_type == "FilterExpression":
            new_name_prefix = "#attribute_name"
            new_value_prefix = ":attribute_value"

        elif expression_type == "ConditionExpression":
            new_name_prefix = "#condition_attribute_name"
            new_value_prefix = ":condition_attribute_value"

        else:
            raise TypeError(
                "Invalid Condition type. Must be one of KeyConditionExpression, ConditionExpression, or FilterExpression"
            )

        for key, val in raw_exp.attribute_name_placeholders.items():
            new_key = key.replace(original_name_prefix, new_name_prefix)
            opts["ExpressionAttributeNames"][new_key] = val
            opts[expression_type] = opts[expression_type].replace(key, new_key)

        for key, val in raw_exp.attribute_value_placeholders.items():
            new_key = key.replace(original_value_prefix, new_value_prefix)
            opts["ExpressionAttributeValues"][new_key] = val
            opts[expression_type] = opts[expression_type].replace(key, new_key)

        opts["TableName"] = self._config.table

        return opts

    @classmethod
    def _construct_from_db(cls, item: Dict) -> DynamojoModel:
        """
        Rehydrates an object from an item out of the DB.
        """
        item = cls._deserialize_dynamo(item)
        res = {}

        for attr, val in item.items():
            if not (
                attr in cls._config.__index_keys__
                or attr in cls._config.joined_attributes
            ):
                res[attr] = val

        if not cls._config.store_aliases:
            for index, alias in cls._config.__index_aliases__.items():
                res[alias] = item[index]

        return cls.construct(**(res))

    def delete(self) -> None:
        """
        Deletes an item from the table
        """
        key = {
            self._config.indexes.table.partitionkey: self.__index_values__[
                self._config.indexes.table.partitionkey
            ]
        }

        if self._config.indexes.table.is_composit:
            key[self._config.indexes.table.sortkey] = self.__index_values__[
                self._config.indexes.table.sortkey
            ]

        res = self._config.table.delete_item(Key=key)

        return res

    @staticmethod
    def _deserialize_dynamo(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deserializes the results from a low-level boto3 Dynamodb client query/get_item
        into a standard dictionary.
        """
        return {k: TypeDeserializer().deserialize(v) for k, v in data.items()}

    @classmethod
    def fetch(cls, pk: str, sk: str = None, **kwargs: Dict[str, Any]) -> DynamojoModel:
        """
        Returns a rehydrated object from the database
        """

        key = {cls._config.indexes.table.partitionkey: pk}

        if cls._config.indexes.table.sortkey:
            key[cls._config.indexes.table.sortkey] = sk

        serialized_key = {k: TypeSerializer().serialize(v) for k, v in key.items()}

        opts = {"Key": serialized_key, "TableName": cls._config.table, **kwargs}

        res = DYNAMOCLIENT.get_item(**opts)

        if item := res.get("Item"):
            return cls._construct_from_db(item)

    @classmethod
    def get_index_by_name(cls, name: str) -> Index:
        """
        Accepts a string as a name and returns an Index() object
        """
        try:
            return cls._config.indexes[name]
        except KeyError:
            raise IndexNotFoundError(f"Index {name} does not exist")

    def index_attributes(self) -> Dict[str, Any]:
        """
        Returns a dict containing index attributes as keys along with their set values
        """
        indexes = {}
        for mapping in self._config.index_maps:
            if hasattr(mapping, "partitionkey"):
                indexes[mapping.index.partitionkey] = self.__getattribute__(
                    mapping.partitionkey
                )
            if hasattr(mapping, "sortkey"):
                indexes[mapping.index.sortkey] = self.__getattribute__(mapping.sortkey)
        return indexes

    def item(self) -> Dict[str, Any]:
        """
        Returns a dict that contains declared attributes and attributes dynamically generated
        by self._config.joined_attributes
        """
        return {**self.dict(), **self.joined_attributes()}

    def joined_attributes(self) -> Dict[str, str]:
        """
        Returns a dict of attributes created dynamically by self._config.joined_attributes
        """
        return {
            attr: self.__getattribute__(attr) for attr in self._config.joined_attributes
        }

    @classmethod
    def _mutate_attribute(cls, field: str, val: Any) -> Any:
        """
        Returns the mutated value using the callable specified in cls._config.mutators for
        an attribute.
        """
        return super().__setattr__(
            field, cls._config.mutators[field].callable(field, val, cls)
        )

    def _prepare_db_item(self):
        """
        Serializes self.item() for storage in the database using the low-level dynamodb client.
        """
        item = {
            k: TypeSerializer().serialize(v)
            for k, v in self._db_item().items()
        }

        if not self._config.store_aliases:
            for alias in self._config.__index_aliases__.values():
                if alias in item:
                    del item[alias]

        return  item

    @classmethod
    def query(
        cls,
        KeyConditionExpression: ConditionBase,
        Index: Union[Index, str] = None,
        FilterExpression: AttributeBase = None,
        Limit: int = 1000,
        ExclusiveStartKey: dict = None,
        result_type: str = "standard",
        **kwargs: Dict[str, Any],
    ) -> QueryResults:
        """
        Runs a Dynamodb query using a condition from db.Index. The kwargs argument can be any
        boto3.client("dynamodb").query() argument that is not explicitely defined in the signature.
        """

        if result_type not in ("standard", "deserialized", "raw"):
            raise ValueError("Argument 'result_type' must be one of standard, raw, or deserialized")

        opts = {**kwargs, "Limit": Limit}

        opts.update(
            cls._get_raw_condition_expression(exp=KeyConditionExpression, index=Index)
        )

        if FilterExpression is not None:
            filter_opts = cls._get_raw_condition_expression(
                exp=FilterExpression, expression_type="FilterExpression"
            )
            opts["ExpressionAttributeNames"].update(
                filter_opts.pop("ExpressionAttributeNames")
            )
            opts["ExpressionAttributeValues"].update(
                filter_opts.pop("ExpressionAttributeValues")
            )
            opts.update(filter_opts)

        if ExclusiveStartKey is not None:
            opts["ExclusiveStartKey"] = ExclusiveStartKey

        msg = (
            f"Querying with index `{opts['IndexName']}`"
            if opts.get("IndexName")
            else "Querying with table index"
        )

        getLogger().info(msg)

        res = DYNAMOCLIENT.query(**opts)
        
        res["Items"] = [cls._construct_from_db(item) for item in res["Items"]]
        return QueryResults(**res)

    def save(
        self, ConditionExpression: ConditionBase = None, **kwargs: Dict[str, Any]
    ) -> None:
        """
        Stores our item in Dynamodb
        """

        item = self._prepare_db_item()

        opts = {"TableName": self._config.table, "Item": item, **kwargs}

        if ConditionExpression:
            exp = self._get_raw_condition_expression(
                ConditionExpression, expression_type="ConditionExpression"
            )
            opts.update(exp)

        return DYNAMOCLIENT.put_item(**opts)


@dataclass
class QueryResults(UserDict):
    Items: List[DynamojoModel]
    Count: int
    ResponseMetadata: Dict[str, Any]
    ScannedCount: int
    LastEvaluatedKey: Dict[str, Dict[str, Any]] = None
    ConsumedCapacity: Dict[str, Any] = None


DynamojoModel = TypeVar("DynamojoModel", bound=DynamojoBase)

