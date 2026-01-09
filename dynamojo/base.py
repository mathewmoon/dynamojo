#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from typing import Any

from boto3.dynamodb.conditions import (
    AttributeBase,
    ConditionBase,
    ConditionExpressionBuilder,
)
from boto3.dynamodb.types import Binary, Decimal
from pydantic import BaseModel

from .index import Index, Lsi, Gsi
from .config import DynamojoConfig
from .exceptions import StaticAttributeError, IndexNotFoundError
from .utils import Delta, TYPE_SERIALIZER, TYPE_DESERIALIZER


class DynamojoBase(BaseModel, ABC):
    """A class to use as a base for modeling objects to store in Dynamodb. This class
    is intended to be inherited by another class, which actually defines the model.
    """

    #: All subclasses should override with their own method that returns config of the type `:class:dynamojo.config.DynamojoConfig`
    @classmethod
    @abstractmethod
    def _config(cls) -> DynamojoConfig:
        pass

    #: Original object
    _original: DynamojoBase

    def __new__(cls: DynamojoBase, *_: list[Any], **__: dict[Any, Any]) -> DynamojoBase:
        if cls is DynamojoBase:
            raise TypeError(f"{cls} must be subclassed")
        return super().__new__(cls)

    def __init__(self: DynamojoBase, **kwargs: dict[str, Any]) -> None:
        """Initialize a new object with any required or optional attributes"""
        super().__init__(**kwargs)

        for attr in self._config().joined_attributes:
            if attr.attribute in self.model_dump():
                raise AttributeError(
                    f"Attribute '{attr}' cannot be declared as a member and a joined field"
                )
        for attr in self._config()._index_keys:
            if attr in self.model_dump():
                raise AttributeError(
                    f"Attribute {attr} is part of an index and cannot be declared directly. Use IndexMap() and an alias instead"
                )

        # TODO: Flush out these mutators.
        for k, v in kwargs.items():
            if k in self._config().mutators:
                kwargs[k] = self._mutate_attribute(k, v)

        self._original = deepcopy(self)

    @property
    def _deepdiff(self):
        return Delta(new=self, old=self._original, deep=True)

    @property
    def _diff(self):
        return Delta(new=self, old=self._original, deep=False)

    def __getattribute__(self: DynamojoBase, name: str) -> Any:
        if super().__getattribute__("_config")().__joined_attributes__.get(name):
            return self._generate_joined_attribute(name)

        return super().__getattribute__(name)

    @property
    def _has_changed(self):
        return self._deepdiff.hasChanged

    def __setattr__(self: DynamojoBase, field: str, val: Any) -> None:
        # Mutations should happen first to allow for other dynamic fields to get their updated value
        if field in self._config().mutators:
            val = self._mutate_attribute(field, val)

        if field in self._config().__joined_attributes__:
            raise AttributeError(
                f"Attribute '{field}' is a joined field and cannot be set directly"
            )

        if field in self._config()._index_keys:
            raise AttributeError(
                f"Attribute '{field}' is an index key and cannot be set directly. Use IndexMap() to create an alias"
            )

        # Static fields can only be set once
        if (
            field in self._config().static_attributes
            and hasattr(self, field)
            and self.__getattribute__(field) != val
        ):
            raise StaticAttributeError(f"Attribute '{field}' is immutable.")

        return super().__setattr__(field, val)

    def _db_item(self) -> dict[str, Any]:
        """
        Returns the item exactly as it is in the database. If self._config().store_aliases is False
        then those aliases will be omitted.
        """
        item = {
            **self.model_dump(),
            **self.joined_attributes(),
            **self.index_attributes(),
        }
        if not self._config().store_aliases:
            for attr in self._config()._index_aliases.values():
                if attr in item:
                    del item[attr]
        return item

    @classmethod
    def _convert_dynamo_types(
        cls: DynamojoBase, db_item: dict[str, Any]
    ) -> dict[str, Any]:
        def decimal(obj: Any) -> Any:
            if isinstance(obj, Decimal):
                if obj % 1 == 0:
                    return int(obj)
                else:
                    return float(obj)
            return obj

        def deserialize(item: Any) -> Any:
            if isinstance(item, dict):
                return {k: deserialize(v) for k, v in item.items()}

            if isinstance(item, list):
                return [deserialize(v) for v in item]

            if isinstance(item, Binary):
                return item.value

            if isinstance(item, Decimal):
                return decimal(item)

            return item

        for k, v in db_item.items():
            db_item[k] = deserialize(v)
        return db_item

    def _generate_joined_attribute(self: DynamojoBase, name: str) -> str:
        """
        Takes attribute names defined in self._config().joined_attributes and stores them with the values
        of the corresponding attributes concatenated by JoinedAttribute.separator.
        """
        item = super().__getattribute__("dict")()
        joiner = super().__getattribute__("_config")().__joined_attributes__[name]
        sources = joiner.fields
        separator = joiner.separator
        new_val = [item.get(source, "") for source in sources]
        return separator.join(new_val)

    @classmethod
    def _get_index_from_attributes(
        cls, partitionkey: str = None, sortkey: str = None
    ) -> Index:
        """
        Returns an Index() object based on attributes being passed as arguments.
        If multiple indexes match then the table index, if matched is returned first. If
        not the table index then the first match in the list.
        """
        lsi_indexes = [x for x in cls._config().index_maps if isinstance(x.index, Lsi)]
        gsi_indexes = [x for x in cls._config().index_maps if isinstance(x.index, Gsi)]

        # Need to handle GSI's without sortkeys
        table_pks = [cls._config().indexes.table.partitionkey]
        pk_alias = cls._config()._index_aliases.get(
            cls._config().indexes.table.partitionkey
        )
        if pk_alias:
            table_pks.append(pk_alias)

        table_sks = [cls._config().indexes.table.sortkey]
        sk_alias = cls._config()._index_aliases.get(cls._config().indexes.table.sortkey)

        if sk_alias:
            table_sks.append(sk_alias)

        if partitionkey in table_pks and (sortkey in table_sks or sortkey is None):
            return cls._config().indexes.table

        for idx in lsi_indexes:
            if partitionkey in table_pks and sortkey == idx.sortkey:
                return idx.index

        for idx in gsi_indexes:
            if partitionkey == idx.partitionkey and (
                sortkey == idx.sortkey or sortkey is None
            ):
                return idx.index

        raise IndexNotFoundError(
            "Could not find a suitable index. Either specify a valid index or change the Condition statement"
        )

    @classmethod
    def _get_raw_condition_expression(
        self,
        exp: ConditionBase,
        index: Index | str = None,
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
                # The problem here is that if the index uses an aliase we don't get the correct
                # key names

            for placeholder, attr in raw_exp.attribute_name_placeholders.items():
                if attr == attribute_names[0]:
                    if type(index) == Lsi:
                        partitionkey = self._config().indexes.table.partitionkey
                    else:
                        partitionkey = index.partitionkey

                    raw_exp.attribute_name_placeholders[placeholder] = partitionkey
                if len(attribute_names) == 2 and attr == attribute_names[1]:
                    raw_exp.attribute_name_placeholders[placeholder] = index.sortkey

        for k, v in raw_exp.attribute_value_placeholders.items():
            raw_exp.attribute_value_placeholders[k] = TYPE_SERIALIZER.serialize(v)

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

        opts["TableName"] = self._config().table

        return opts

    @classmethod
    async def execute_write_transaction(cls, expressions: list[dict], **opts) -> dict:
        res = cls._config().dynamo_client.transact_write_items(
            TransactItems=expressions, **opts
        )
        return res

    @classmethod
    def _construct_from_db(cls, item: dict) -> DynamojoBase:
        """
        Rehydrates an object from an item out of the DB.
        """
        item = cls._deserialize_dynamo(item)
        if cls._config().convert_dynamodb_types:
            item = cls._convert_dynamo_types(item)

        res = {}

        for attr, val in item.items():
            if not (
                attr in cls._config()._index_keys
                or attr in cls._config().__joined_attributes__
            ):
                res[attr] = val

        if not cls._config().store_aliases:
            for index, alias in cls._config()._index_aliases.items():
                res[alias] = item[index]

        res = cls.model_validate(res)
        # IMPORTANT: All privateattrs have to be set AFTER model_validate() because
        # model_construct() does not call __init__()
        res._original = deepcopy(res)
        return res

    async def delete(self) -> None:
        """
        Deletes an item from the table
        """
        key = {
            self._config().indexes.table.partitionkey: self.index_attributes()[
                self._config().indexes.table.partitionkey
            ]
        }

        if self._config().indexes.table.is_composite:
            key[self._config().indexes.table.sortkey] = self.index_attributes()[
                self._config().indexes.table.sortkey
            ]

        serialized_key = {k: TYPE_SERIALIZER.serialize(v) for k, v in key.items()}

        res = self._config().dynamo_client.delete_item(
            Key=serialized_key, TableName=self._config().table
        )

        return res

    @staticmethod
    def _deserialize_dynamo(data: dict[str, Any]) -> dict[str, Any]:
        """
        Deserializes the results from a low-level boto3 Dynamodb client query/get_item
        into a standard dictionary.
        """
        return {k: TYPE_DESERIALIZER.deserialize(v) for k, v in data.items()}

    @classmethod
    async def batch_get_item(cls, keys: list[dict[str, Any]]) -> list[dict[str, Any]]:
        keys = [
            await cls.fetch(pk=key["pk"], sk=key["sk"], key_only=True) for key in keys
        ]
        batches = [keys[i : i + 100] for i in range(0, len(keys), 100)]
        getLogger().debug(f"Batching {len(keys)} keys into {len(batches)} batches")
        results = []

        for batch in batches:
            res = cls._config().dynamo_client.batch_get_item(
                RequestItems={cls._config().table: {"Keys": batch}}
            )
            results += res["Responses"][cls._config().table]

            while unprocessed_keys := res.get("UnprocessedKeys"):
                res = cls._config().dynamo_client.batch_get_item(
                    RequestItems={cls._config().table: {"Keys": unprocessed_keys}}
                )
                results += res["Responses"][cls._config().table]

        return [cls._construct_from_db(item) for item in results]

    @classmethod
    async def fetch(
        cls, pk: str, sk: str = None, key_only: bool = False, **kwargs: dict[str, Any]
    ) -> DynamojoBase:
        """
        Returns a rehydrated object from the database
        """

        key = {cls._config().indexes.table.partitionkey: pk}

        if cls._config().indexes.table.sortkey:
            key[cls._config().indexes.table.sortkey] = sk

        serialized_key = {k: TYPE_SERIALIZER.serialize(v) for k, v in key.items()}

        opts = {"Key": serialized_key, "TableName": cls._config().table, **kwargs}

        if key_only:
            return opts["Key"]

        res = cls._config().dynamo_client.get_item(**opts)

        if item := res.get("Item"):
            return cls._construct_from_db(item)

    @classmethod
    def get_index_by_name(cls, name: str) -> Index:
        """
        Accepts a string as a name and returns an Index() object
        """
        try:
            return cls._config().indexes[name]
        except KeyError:
            raise IndexNotFoundError(f"Index {name} does not exist")

    def index_attributes(self) -> dict[str, Any]:
        """
        Returns a dict containing index attributes as keys along with their set values
        """
        indexes = {}
        for mapping in self._config().index_maps:
            if hasattr(mapping, "partitionkey"):
                indexes[mapping.index.partitionkey] = self.__getattribute__(
                    mapping.partitionkey
                )
            if hasattr(mapping, "sortkey"):
                indexes[mapping.index.sortkey] = self.__getattribute__(mapping.sortkey)
        return indexes

    def item(self) -> dict[str, Any]:
        """
        Returns a dict that contains declared attributes and attributes dynamically generated
        by self._config().joined_attributes
        """
        return {**self.model_dump(), **self.joined_attributes()}

    def joined_attributes(self) -> dict[str, str]:
        """
        Returns a dict of attributes created dynamically by self._config().joined_attributes
        """
        return {
            attr: self.__getattribute__(attr)
            for attr in self._config().__joined_attributes__
        }

    @classmethod
    def _mutate_attribute(cls, field: str, val: Any) -> Any:
        """
        Returns the mutated value using the callable specified in cls._config().mutators for
        an attribute.
        """
        return super().__setattr__(
            field, cls._config().mutators[field].callable(field, val, cls)
        )

    def _prepare_db_item(self):
        """
        Serializes self.item() for storage in the database using the low-level dynamodb client.
        """
        item = {k: TYPE_SERIALIZER.serialize(v) for k, v in self._db_item().items()}

        if not self._config().store_aliases:
            for alias in self._config()._index_aliases.values():
                if alias in item:
                    del item[alias]

        return item

    @classmethod
    async def query(
        cls,
        KeyConditionExpression: ConditionBase,
        Index: Index | str = None,
        FilterExpression: AttributeBase = None,
        Limit: int = 1000,
        ExclusiveStartKey: dict = None,
        result_type: str = "standard",
        **kwargs: dict[str, Any],
    ) -> QueryResults:
        """
        Runs a Dynamodb query using a condition from db.Index. The kwargs argument can be any
        boto3.client("dynamodb").query() argument that is not explicitely defined in the signature.
        """

        if result_type not in ("standard", "deserialized", "raw"):
            raise ValueError(
                "Argument 'result_type' must be one of standard, raw, or deserialized"
            )

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

        getLogger().debug(msg)

        res = cls._config().dynamo_client.query(**opts)

        res["Items"] = [cls._construct_from_db(item) for item in res["Items"]]
        return QueryResults(**res)

    def make_put_item_opts(
        self,
        ConditionExpression: ConditionBase = None,
        fail_on_exists: bool = True,
        **kwargs,
    ):
        table_pk = self._config().indexes.table.partitionkey
        table_sk = self._config().indexes.table.sortkey

        item = self._prepare_db_item()

        opts = {"TableName": self._config().table, "Item": item, **kwargs}

        if ConditionExpression:
            exp = self._get_raw_condition_expression(
                ConditionExpression, expression_type="ConditionExpression"
            )
            opts.update(exp)

        if fail_on_exists:
            sk_expression = f"attribute_not_exists({table_sk})"
            pk_expression = f"attribute_not_exists({table_pk})"
            fail_expression = (
                f"({pk_expression} AND {sk_expression}) "
                if table_sk is not None
                else pk_expression
            )
            if ConditionExpression:
                opts["ConditionExpression"] = (
                    f"{fail_expression} AND {opts['ConditionExpression']}"
                )
            else:
                opts["ConditionExpression"] = fail_expression

        return opts

    async def save(
        self,
        ConditionExpression: ConditionBase = None,
        fail_on_exists: bool = True,
        **kwargs: dict[str, Any],
    ) -> None:
        """
        Stores our item in Dynamodb
        """
        opts = self.make_put_item_opts(
            ConditionExpression=ConditionExpression,
            fail_on_exists=fail_on_exists,
            **kwargs,
        )
        return self._config().dynamo_client.put_item(**opts)

    def make_update_opts(self, pk_name: str = "pk", sk_name: str = "sk", **opts):
        diff = self._diff
        attribute_names = {}
        attribute_values = {}
        set_statement_items = []
        del_statement_items = []
        set_items = {**diff.added, **diff.changed}
        key = {pk_name: TYPE_SERIALIZER.serialize(self._db_item()[pk_name])}
        if sk_name is not None:
            key[sk_name] = TYPE_SERIALIZER.serialize(self._db_item()[sk_name])

        for attr, val in self._db_item().items():
            if attr in diff.keys:
                attribute_names[f"#{attr}"] = attr
                attribute_values[f":{attr}"] = val
                if attr in set_items.keys():
                    statement = f"#{attr} = :{attr}"
                    set_statement_items.append(statement)
                else:
                    del_statement_items.append(statement)

        set_statement = (
            f"SET {', '.join(set_statement_items)}" if set_statement_items else ""
        )
        del_statement = (
            f"REMOVE {', '.join(del_statement_items)}" if del_statement_items else ""
        )

        opts["TableName"] = self._config().table
        opts["Key"] = key
        opts["ExpressionAttributeNames"] = attribute_names
        opts["ExpressionAttributeValues"] = {
            k: TYPE_SERIALIZER.serialize(v) for k, v in attribute_values.items()
        }

        opts["UpdateExpression"] = f"{set_statement} {del_statement}"
        if condition_expression := opts.get("ConditionExpression"):
            exp = self._get_raw_condition_expression(
                exp=condition_expression,
                expression_type="ConditionExpression",
            )
            opts["ExpressionAttributeNames"].update(exp["ExpressionAttributeNames"])
            opts["ExpressionAttributeValues"].update(exp["ExpressionAttributeValues"])
            opts["ConditionExpression"] = exp["ConditionExpression"]

        return opts

    async def update(self, **opts):
        diff = self._diff
        if not self._diff.hasChanged:
            return None
        pk_name = self._config().indexes.table.partitionkey
        sk_name = self._config().indexes.table.sortkey
        if pk_name in diff.keys or sk_name in diff.keys:
            raise AttributeError(
                "Cannot update table key attributes. Use `self.save()` instead."
            )

        opts = self.make_update_opts(pk_name=pk_name, sk_name=sk_name, **opts)
        self._config().dynamo_client.update_item(**opts)
        return self


@dataclass
class QueryResults(UserDict):
    Items: list[DynamojoBase]
    Count: int
    ResponseMetadata: dict[str, Any]
    ScannedCount: int
    LastEvaluatedKey: dict[str, dict[str, Any]] = None
    ConsumedCapacity: dict[str, Any] = None
