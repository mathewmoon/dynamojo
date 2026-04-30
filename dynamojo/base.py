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


# ---------------------------------------------------------------------------
# Dict-path helpers — shared by standalone methods and make_update_opts.
#
# WARNING: dots inside a key name (e.g. {"some.key": 1}) are indistinguishable
# from path separators.  Use only attribute names that do not contain literal
# dots — which is strongly recommended by DynamoDB conventions anyway.
# ---------------------------------------------------------------------------

def _dict_path_names(field_name: str, path: str) -> tuple[dict, str]:
    """Return (ExpressionAttributeNames, expression_path) for a dot-separated path.

    Example: field_name="mydict", path="a.b.c" →
      names   = {"#mydict": "mydict", "#mydict__a": "a",
                 "#mydict__a__b": "b", "#mydict__a__b__c": "c"}
      expr    = "#mydict.#mydict__a.#mydict__a__b.#mydict__a__b__c"
    """
    parts = path.split(".")
    names: dict[str, str] = {f"#{field_name}": field_name}
    key = field_name
    placeholders = [f"#{field_name}"]
    for part in parts:
        key = f"{key}__{part}"
        ph = f"#{key}"
        names[ph] = part
        placeholders.append(ph)
    return names, ".".join(placeholders)


def _deep_set(d: dict, parts: list[str], value: Any) -> None:
    for part in parts[:-1]:
        d = d.setdefault(part, {})
    d[parts[-1]] = value


def _deep_remove(d: dict, parts: list[str]) -> None:
    for part in parts[:-1]:
        if not isinstance(d := d.get(part), dict):
            return
    d.pop(parts[-1], None)


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

    async def delete(
        self,
        ConditionExpression: ConditionBase = None,
        **kwargs,
    ) -> None:
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

        opts = {"Key": serialized_key, "TableName": self._config().table, **kwargs}

        if ConditionExpression:
            exp = self._get_raw_condition_expression(
                ConditionExpression, expression_type="ConditionExpression"
            )
            opts.update(exp)

        res = self._config().dynamo_client.delete_item(**opts)

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

    @classmethod
    def _resolve_alias_targets(cls, field: str) -> list[str]:
        """Translate a logical field name into the attribute names to write.

        If `field` is registered as an IndexMap alias (i.e., it appears as a
        value in `_index_aliases`), the underlying index attribute name(s) are
        returned — plus the alias itself when `store_aliases` is True, so the
        alias and its backing index key stay in sync. Non-aliased fields are
        returned as-is. Multiple GSIs aliasing the same logical field expand
        to a target per GSI.
        """
        aliases = cls._config()._index_aliases  # {index_key_name: alias_name}
        targets = [key for key, alias in aliases.items() if alias == field]
        if not targets:
            return [field]
        if cls._config().store_aliases:
            targets.append(field)
        return targets

    @classmethod
    def _build_atomic_update_clauses(
        cls,
        set: dict[str, Any] | None = None,
        list_append: dict[str, list] | None = None,
        list_prepend: dict[str, list] | None = None,
        list_remove: dict[str, int] | None = None,
        list_set: dict[str, tuple[int, Any]] | None = None,
        number_add: dict[str, int | float] | None = None,
        dict_set: dict[str, Any] | None = None,
        dict_remove: list[str] | None = None,
        set_if_not_exists: dict[str, Any] | None = None,
    ) -> tuple[dict, dict, list[str], list[str], list[str]]:
        """Build expression fragments for the atomic-op kwargs.

        Returns (attribute_names, attribute_values, set_items, del_items, add_items).
        Values are NOT yet serialized — caller is responsible for serializing
        attribute_values via TYPE_SERIALIZER before handing them to boto3.

        Each user-supplied field name is run through `_resolve_alias_targets`,
        so aliased GSI/LSI keys propagate to all of their underlying index
        attributes automatically.
        """
        attribute_names: dict[str, str] = {}
        attribute_values: dict[str, Any] = {}
        set_items: list[str] = []
        del_items: list[str] = []
        add_items: list[str] = []

        # `set` — plain top-level field assignment ("SET #f = :_set_f")
        for field, value in (set or {}).items():
            coerced = Decimal(str(value)) if isinstance(value, float) else value
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                attribute_values[f":_set_{target}"] = coerced
                set_items.append(f"#{target} = :_set_{target}")

        for field, values in (list_append or {}).items():
            vals = list(values)
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                attribute_values[f":_la_{target}"] = vals
                set_items.append(f"#{target} = list_append(#{target}, :_la_{target})")

        for field, values in (list_prepend or {}).items():
            vals = list(values)
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                attribute_values[f":_lp_{target}"] = vals
                set_items.append(f"#{target} = list_append(:_lp_{target}, #{target})")

        # list_remove — index embedded in path, no value placeholder
        for field, index in (list_remove or {}).items():
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                del_items.append(f"#{target}[{index}]")

        # list_set — index embedded in path
        for field, (index, value) in (list_set or {}).items():
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                attribute_values[f":_ls_{target}"] = value
                set_items.append(f"#{target}[{index}] = :_ls_{target}")

        # number_add — goes in ADD clause; convert float → Decimal for the serializer
        for field, delta in (number_add or {}).items():
            coerced = Decimal(str(delta)) if isinstance(delta, float) else delta
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                attribute_values[f":_na_{target}"] = coerced
                add_items.append(f"#{target} :_na_{target}")

        # dict_set — flat "field.path" keys, value written at terminus
        for full_path, v in (dict_set or {}).items():
            field, _, path = full_path.partition(".")
            if not path:
                raise ValueError(
                    f"dict_set key '{full_path}' must include a dot-separated path, e.g. 'field.key'"
                )
            for target in cls._resolve_alias_targets(field):
                names, expr_path = _dict_path_names(target, path)
                vk = f":_ds_{target}__{path.replace('.', '__')}"
                attribute_names.update(names)
                attribute_values[vk] = v
                set_items.append(f"{expr_path} = {vk}")

        # dict_remove — list of flat "field.path" strings
        for full_path in (dict_remove or []):
            field, _, path = full_path.partition(".")
            if not path:
                raise ValueError(
                    f"dict_remove path '{full_path}' must include a dot-separated path, e.g. 'field.key'"
                )
            for target in cls._resolve_alias_targets(field):
                names, expr_path = _dict_path_names(target, path)
                attribute_names.update(names)
                del_items.append(expr_path)

        for field, value in (set_if_not_exists or {}).items():
            for target in cls._resolve_alias_targets(field):
                attribute_names[f"#{target}"] = target
                attribute_values[f":_ine_{target}"] = value
                set_items.append(f"#{target} = if_not_exists(#{target}, :_ine_{target})")

        return attribute_names, attribute_values, set_items, del_items, add_items

    def make_update_opts(
        self,
        pk_name: str = "pk",
        sk_name: str = "sk",
        set: dict[str, Any] | None = None,
        list_append: dict[str, list] | None = None,
        list_prepend: dict[str, list] | None = None,
        list_remove: dict[str, int] | None = None,
        list_set: dict[str, tuple[int, Any]] | None = None,
        number_add: dict[str, int | float] | None = None,
        dict_set: dict[str, Any] | None = None,
        dict_remove: list[str] | None = None,
        set_if_not_exists: dict[str, Any] | None = None,
        **opts,
    ):
        # NOTE: `set` shadows the built-in inside this function — use the
        # builtins module if a real set() is ever needed below.
        import builtins
        atomic_fields = builtins.set(
            list(set or {})
            + list(list_append or {})
            + list(list_prepend or {})
            + list(list_remove or {})
            + list(list_set or {})
            + list(number_add or {})
            + [fp.partition(".")[0] for fp in (dict_set or {})]
            + [fp.partition(".")[0] for fp in (dict_remove or [])]
            + list(set_if_not_exists or {})
        )

        diff = self._diff
        diff_set_items = {**diff.added, **diff.changed}

        (
            attribute_names,
            attribute_values,
            set_statement_items,
            del_statement_items,
            add_statement_items,
        ) = self._build_atomic_update_clauses(
            set=set,
            list_append=list_append,
            list_prepend=list_prepend,
            list_remove=list_remove,
            list_set=list_set,
            number_add=number_add,
            dict_set=dict_set,
            dict_remove=dict_remove,
            set_if_not_exists=set_if_not_exists,
        )

        # diff-based SET/REMOVE — skip fields covered by an atomic op
        for attr, val in self._db_item().items():
            if attr in diff.keys and attr not in atomic_fields:
                attribute_names[f"#{attr}"] = attr
                attribute_values[f":{attr}"] = val
                if attr in diff_set_items.keys():
                    statement = f"#{attr} = :{attr}"
                    set_statement_items.append(statement)
                else:
                    del_statement_items.append(statement)

        clauses = filter(None, [
            f"SET {', '.join(set_statement_items)}" if set_statement_items else "",
            f"REMOVE {', '.join(del_statement_items)}" if del_statement_items else "",
            f"ADD {', '.join(add_statement_items)}" if add_statement_items else "",
        ])

        opts["TableName"] = self._config().table
        opts["Key"] = self._build_key()
        opts["ExpressionAttributeNames"] = attribute_names
        opts["UpdateExpression"] = " ".join(clauses)

        serialized_values = {
            k: TYPE_SERIALIZER.serialize(v) for k, v in attribute_values.items()
        }
        if serialized_values:
            opts["ExpressionAttributeValues"] = serialized_values

        if condition_expression := opts.get("ConditionExpression"):
            exp = self._get_raw_condition_expression(
                exp=condition_expression,
                expression_type="ConditionExpression",
            )
            opts["ExpressionAttributeNames"].update(exp["ExpressionAttributeNames"])
            opts.setdefault("ExpressionAttributeValues", {}).update(
                exp["ExpressionAttributeValues"]
            )
            opts["ConditionExpression"] = exp["ConditionExpression"]

        return opts

    @classmethod
    async def update_by_key(
        cls,
        pk: Any,
        sk: Any = None,
        *,
        set: dict[str, Any] | None = None,
        list_append: dict[str, list] | None = None,
        list_prepend: dict[str, list] | None = None,
        list_remove: dict[str, int] | None = None,
        list_set: dict[str, tuple[int, Any]] | None = None,
        number_add: dict[str, int | float] | None = None,
        dict_set: dict[str, Any] | None = None,
        dict_remove: list[str] | None = None,
        set_if_not_exists: dict[str, Any] | None = None,
        ConditionExpression: ConditionBase | None = None,
        **opts,
    ) -> dict:
        """Issue a true DynamoDB UpdateItem identified by primary key.

        No upfront GetItem and no instance state — useful for stream handlers
        and conditionally-guarded mutations where only the key is in hand.
        At least one update-producing kwarg must be supplied (`set` or any of
        the atomic ops); a ConditionExpression alone is not a valid UpdateItem.
        """
        if not any([
            set, list_append, list_prepend, list_remove, list_set,
            number_add, dict_set, dict_remove, set_if_not_exists,
        ]):
            raise ValueError(
                "update_by_key requires at least one update-producing kwarg "
                "(set, list_append, list_prepend, list_remove, list_set, "
                "number_add, dict_set, dict_remove, set_if_not_exists)"
            )

        (
            attribute_names,
            attribute_values,
            set_items,
            del_items,
            add_items,
        ) = cls._build_atomic_update_clauses(
            set=set,
            list_append=list_append,
            list_prepend=list_prepend,
            list_remove=list_remove,
            list_set=list_set,
            number_add=number_add,
            dict_set=dict_set,
            dict_remove=dict_remove,
            set_if_not_exists=set_if_not_exists,
        )

        clauses = filter(None, [
            f"SET {', '.join(set_items)}" if set_items else "",
            f"REMOVE {', '.join(del_items)}" if del_items else "",
            f"ADD {', '.join(add_items)}" if add_items else "",
        ])

        opts["TableName"] = cls._config().table
        opts["Key"] = cls._build_key_from_args(pk, sk)
        opts["UpdateExpression"] = " ".join(clauses)
        opts["ExpressionAttributeNames"] = attribute_names

        if attribute_values:
            opts["ExpressionAttributeValues"] = {
                k: TYPE_SERIALIZER.serialize(v) for k, v in attribute_values.items()
            }

        if ConditionExpression is not None:
            exp = cls._get_raw_condition_expression(
                exp=ConditionExpression,
                expression_type="ConditionExpression",
            )
            opts["ExpressionAttributeNames"].update(exp["ExpressionAttributeNames"])
            opts.setdefault("ExpressionAttributeValues", {}).update(
                exp["ExpressionAttributeValues"]
            )
            opts["ConditionExpression"] = exp["ConditionExpression"]

        return cls._config().dynamo_client.update_item(**opts)

    @classmethod
    async def delete_by_key(
        cls,
        pk: Any,
        sk: Any = None,
        *,
        ConditionExpression: ConditionBase | None = None,
        **opts,
    ) -> dict:
        """Issue a true DynamoDB DeleteItem identified by primary key.

        No upfront GetItem and no instance state.
        """
        opts["TableName"] = cls._config().table
        opts["Key"] = cls._build_key_from_args(pk, sk)

        if ConditionExpression is not None:
            exp = cls._get_raw_condition_expression(
                exp=ConditionExpression,
                expression_type="ConditionExpression",
            )
            opts.update(exp)

        return cls._config().dynamo_client.delete_item(**opts)

    async def update(
        self,
        set: dict[str, Any] | None = None,
        list_append: dict[str, list] | None = None,
        list_prepend: dict[str, list] | None = None,
        list_remove: dict[str, int] | None = None,
        list_set: dict[str, tuple[int, Any]] | None = None,
        number_add: dict[str, int | float] | None = None,
        dict_set: dict[str, Any] | None = None,
        dict_remove: list[str] | None = None,
        set_if_not_exists: dict[str, Any] | None = None,
        **opts,
    ):
        has_atomic = any([
            set, list_append, list_prepend, list_remove, list_set,
            number_add, dict_set, dict_remove, set_if_not_exists,
        ])
        diff = self._diff
        if not diff.hasChanged and not has_atomic:
            return None

        pk_name = self._config().indexes.table.partitionkey
        sk_name = self._config().indexes.table.sortkey
        if pk_name in diff.keys or sk_name in diff.keys:
            raise AttributeError(
                "Cannot update table key attributes. Use `self.save()` instead."
            )

        opts = self.make_update_opts(
            pk_name=pk_name,
            sk_name=sk_name,
            set=set,
            list_append=list_append,
            list_prepend=list_prepend,
            list_remove=list_remove,
            list_set=list_set,
            number_add=number_add,
            dict_set=dict_set,
            dict_remove=dict_remove,
            set_if_not_exists=set_if_not_exists,
            **opts,
        )
        self._config().dynamo_client.update_item(**opts)

        for field, value in (set or {}).items():
            self._sync_field(field, value)
        for field, values in (list_append or {}).items():
            self._sync_field(field, list(getattr(self, field)) + list(values))
        for field, values in (list_prepend or {}).items():
            self._sync_field(field, list(values) + list(getattr(self, field)))
        for field, index in (list_remove or {}).items():
            new = list(getattr(self, field))
            new.pop(index)
            self._sync_field(field, new)
        for field, (index, value) in (list_set or {}).items():
            new = list(getattr(self, field))
            new[index] = value
            self._sync_field(field, new)
        for field, delta in (number_add or {}).items():
            self._sync_field(field, getattr(self, field) + delta)
        _ds_by_field: dict[str, dict] = {}
        for full_path, value in (dict_set or {}).items():
            field, _, path = full_path.partition(".")
            _ds_by_field.setdefault(field, {})[path] = value
        for field, paths in _ds_by_field.items():
            new_val = deepcopy(getattr(self, field))
            for path, value in paths.items():
                _deep_set(new_val, path.split("."), value)
            self._sync_field(field, new_val)
        _dr_by_field: dict[str, list] = {}
        for full_path in (dict_remove or []):
            field, _, path = full_path.partition(".")
            _dr_by_field.setdefault(field, []).append(path)
        for field, paths in _dr_by_field.items():
            new_val = deepcopy(getattr(self, field))
            for path in paths:
                _deep_remove(new_val, path.split("."))
            self._sync_field(field, new_val)
        for field, value in (set_if_not_exists or {}).items():
            self._sync_field(field, value)

        return self

    # ------------------------------------------------------------------
    # Atomic update helpers
    # ------------------------------------------------------------------

    def _validate_field_type(self, field_name: str, expected_origins: set) -> None:
        import typing
        import types

        if field_name not in type(self).model_fields:
            raise AttributeError(
                f"Field '{field_name}' does not exist on {type(self).__name__}"
            )
        if not expected_origins:
            return
        hints = typing.get_type_hints(type(self))
        annotation = hints[field_name]
        origin = typing.get_origin(annotation)
        if origin is typing.Union or (
            hasattr(types, "UnionType") and isinstance(annotation, types.UnionType)
        ):
            args = typing.get_args(annotation)
            annotation = next(
                (
                    a
                    for a in args
                    if typing.get_origin(a) in expected_origins or a in expected_origins
                ),
                None,
            )
            origin = typing.get_origin(annotation) if annotation else None
        if origin not in expected_origins and annotation not in expected_origins:
            raise TypeError(
                f"Field '{field_name}' on {type(self).__name__} is not of the expected type"
            )

    def _build_key(self) -> dict:
        pk_name = self._config().indexes.table.partitionkey
        sk_name = self._config().indexes.table.sortkey
        key = {pk_name: TYPE_SERIALIZER.serialize(self._db_item()[pk_name])}
        if sk_name is not None:
            key[sk_name] = TYPE_SERIALIZER.serialize(self._db_item()[sk_name])
        return key

    @classmethod
    def _build_key_from_args(cls, pk: Any, sk: Any = None) -> dict:
        """Serialize an explicit (pk, sk) into a DynamoDB Key dict.

        Validates that sk is supplied iff the table is composite.
        """
        pk_name = cls._config().indexes.table.partitionkey
        sk_name = cls._config().indexes.table.sortkey
        key = {pk_name: TYPE_SERIALIZER.serialize(pk)}
        if sk_name is not None:
            if sk is None:
                raise ValueError(
                    f"Table '{cls._config().table}' has sort key '{sk_name}'; "
                    "sk argument is required"
                )
            key[sk_name] = TYPE_SERIALIZER.serialize(sk)
        elif sk is not None:
            raise ValueError(
                f"Table '{cls._config().table}' has no sort key; "
                "sk argument must be omitted"
            )
        return key

    def _atomic_update(
        self,
        update_expression: str,
        attribute_names: dict,
        attribute_values: dict | None,
        condition_expression: ConditionBase = None,
    ) -> None:
        opts = {
            "TableName": self._config().table,
            "Key": self._build_key(),
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": attribute_names,
        }
        if attribute_values:
            opts["ExpressionAttributeValues"] = attribute_values
        if condition_expression is not None:
            exp = self._get_raw_condition_expression(
                exp=condition_expression,
                expression_type="ConditionExpression",
            )
            opts["ExpressionAttributeNames"].update(exp["ExpressionAttributeNames"])
            opts.setdefault("ExpressionAttributeValues", {}).update(
                exp["ExpressionAttributeValues"]
            )
            opts["ConditionExpression"] = exp["ConditionExpression"]
        self._config().dynamo_client.update_item(**opts)

    def _sync_field(self, field_name: str, new_val: Any) -> None:
        setattr(self, field_name, new_val)
        self._original.__dict__[field_name] = deepcopy(new_val)

    # ------------------------------------------------------------------
    # Atomic list operations
    # ------------------------------------------------------------------

    async def list_append(
        self,
        field_name: str,
        values: list,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        self._validate_field_type(field_name, {list})
        self._atomic_update(
            "SET #field = list_append(#field, :items)",
            {"#field": field_name},
            {":items": TYPE_SERIALIZER.serialize(list(values))},
            ConditionExpression,
        )
        self._sync_field(field_name, list(getattr(self, field_name)) + list(values))

    async def list_prepend(
        self,
        field_name: str,
        values: list,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        self._validate_field_type(field_name, {list})
        self._atomic_update(
            "SET #field = list_append(:items, #field)",
            {"#field": field_name},
            {":items": TYPE_SERIALIZER.serialize(list(values))},
            ConditionExpression,
        )
        self._sync_field(field_name, list(values) + list(getattr(self, field_name)))

    async def list_remove(
        self,
        field_name: str,
        index: int,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        self._validate_field_type(field_name, {list})
        self._atomic_update(
            f"REMOVE #field[{index}]",
            {"#field": field_name},
            None,
            ConditionExpression,
        )
        new_val = list(getattr(self, field_name))
        new_val.pop(index)
        self._sync_field(field_name, new_val)

    async def list_set(
        self,
        field_name: str,
        index: int,
        value: Any,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        self._validate_field_type(field_name, {list})
        self._atomic_update(
            f"SET #field[{index}] = :value",
            {"#field": field_name},
            {":value": TYPE_SERIALIZER.serialize(value)},
            ConditionExpression,
        )
        new_val = list(getattr(self, field_name))
        new_val[index] = value
        self._sync_field(field_name, new_val)

    # ------------------------------------------------------------------
    # Atomic numeric operation
    # ------------------------------------------------------------------

    async def number_add(
        self,
        field_name: str,
        delta: int | float,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        self._validate_field_type(field_name, {int, float})
        # boto3 TypeSerializer rejects float; use Decimal for wire encoding
        serializable_delta = Decimal(str(delta)) if isinstance(delta, float) else delta
        self._atomic_update(
            "ADD #field :delta",
            {"#field": field_name},
            {":delta": TYPE_SERIALIZER.serialize(serializable_delta)},
            ConditionExpression,
        )
        self._sync_field(field_name, getattr(self, field_name) + delta)

    # ------------------------------------------------------------------
    # Atomic dict/map operations
    # ------------------------------------------------------------------

    async def dict_set(
        self,
        field_name: str,
        path: str,
        value: Any,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        # NOTE: dots in key names are treated as path separators.
        # See module-level warning in _dict_path_names.
        self._validate_field_type(field_name, {dict})
        names, expr_path = _dict_path_names(field_name, path)
        self._atomic_update(
            f"SET {expr_path} = :value",
            names,
            {":value": TYPE_SERIALIZER.serialize(value)},
            ConditionExpression,
        )
        new_val = deepcopy(getattr(self, field_name))
        _deep_set(new_val, path.split("."), value)
        self._sync_field(field_name, new_val)

    async def dict_remove(
        self,
        field_name: str,
        path: str,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        # NOTE: dots in key names are treated as path separators.
        # See module-level warning in _dict_path_names.
        self._validate_field_type(field_name, {dict})
        names, expr_path = _dict_path_names(field_name, path)
        self._atomic_update(
            f"REMOVE {expr_path}",
            names,
            None,
            ConditionExpression,
        )
        new_val = deepcopy(getattr(self, field_name))
        _deep_remove(new_val, path.split("."))
        self._sync_field(field_name, new_val)

    # ------------------------------------------------------------------
    # Atomic conditional initialiser
    # ------------------------------------------------------------------

    async def set_if_not_exists(
        self,
        field_name: str,
        value: Any,
        ConditionExpression: ConditionBase = None,
    ) -> None:
        self._validate_field_type(field_name, set())
        self._atomic_update(
            "SET #field = if_not_exists(#field, :value)",
            {"#field": field_name},
            {":value": TYPE_SERIALIZER.serialize(value)},
            ConditionExpression,
        )
        # Optimistically assume the field was absent in DynamoDB; if it already
        # existed the DynamoDB write was a no-op and the local value was already
        # correct — caller's responsibility to handle that case.
        self._sync_field(field_name, value)


@dataclass
class QueryResults(UserDict):
    Items: list[DynamojoBase]
    Count: int
    ResponseMetadata: dict[str, Any]
    ScannedCount: int
    LastEvaluatedKey: dict[str, dict[str, Any]] = None
    ConsumedCapacity: dict[str, Any] = None
