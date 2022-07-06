#!/usr/bin/env python3.8
from logging import getLogger
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Union
)

from boto3.dynamodb.conditions import (
    AttributeBase,
    ConditionBase,
    ConditionExpressionBuilder
)
from boto3.dynamodb.types import (
    TypeSerializer,
    TypeDeserializer
)
from pydantic import BaseModel, PrivateAttr
from pydantic.fields import ModelField

from .boto import DYNAMOCLIENT
from .index import (
    Index,
    Lsi
)
from .config import DynamojoConfig
from .exceptions import (
    StaticAttributeError,
    IndexNotFoundError
)


class DynamojoBase(BaseModel):

    _config: DynamojoConfig = PrivateAttr()
    # List of attributes that are part of joins
    __joined_sources__: list = PrivateAttr(default=[])
    # Reserved for internal use
    __reserved__attributes__: ClassVar = [
        "__reserved_attributes__",
        "__joined_sources__"
    ]

    def __init__(self, **kwargs: Dict[str, Any]) -> None:

        for attribute in kwargs.items():
            if attribute in self.__reserved__attributes__:
                raise AttributeError(f"Attribute {attribute} is reserved")

        super().__init__(**kwargs)

        self.__joined_sources__: List = []

        # For any compound attributes we need to reserve their space in self.__fields__
        # and self.__annotations__ or setattr() won't work
        for alias, sources in self._config.joined_attributes.items():
            self.__joined_sources__ += sources
            self.__fields__[alias] = ModelField.infer(
                name=alias,
                value=None,
                annotation=str,
                class_validators=None,
                config=self.__config__,
            )
            self.__annotations__[alias] = str

        for attribute in self.dict():
            if attribute in self.__joined_sources__:
                self.set_compound_attribute(attribute)

        # Index attributes (The real ones in the DB) have to be mapped to either a concrete attribute or
        # one that has been created dynamically by joins
        for index, alias in self._config.__index_aliases__.items():
            if not hasattr(self, alias):
                raise AttributeError(
                    f"Cannot map Index attribute {index} to nonexistent attribute {alias}"
                )

            self.__fields__[index] = self.__fields__[alias]
            self.__annotations__[index] = self.__annotations__[alias]
            super().__setattr__(index, self.__getattribute__(alias))

        # TODO: Flush out these mutators.
        for k, v in kwargs.items():
            if k in self._config.mutators:
                kwargs[k] = self.mutate_attribute(k, v)

    @property
    def item(self) -> Dict:
        return self.dict()

    def mutate_attribute(cls, field: str, val: Any) -> None:
        return super().__setattr__(
            field, cls._config.mutators[field].callable(field, val, cls)
        )

    def __setattr__(self, field: str, val: Any) -> None:
        # Skip all this other stuff if it's an internal
        if field in self.__reserved__attributes__:
            return super().__setattr__(field, val)

        # Mutations should happen first
        if field in self._config.mutators:
            val = self.mutate_attribute(field, val)

        # Static fields can only be set once
        if (
            field in self._config.static_attributes
            and hasattr(self, field)
            and self.__getattribute__(field) != val
        ):
            raise StaticAttributeError(f"Attribute '{field}' is immutable.")

        # Set joined fields
        if field in self.__joined_sources__:
            self.set_compound_attribute(field)

        # Update the index
        if field in self._config.__index_keys__:
            for index, alias in self._config.__index_aliases__.items():
                if field == alias:
                    super().__setattr__(index, val)

        return super().__setattr__(field, val)

    @classmethod
    def fetch(cls, pk: str, sk: str = None, **kwargs) -> Dict:
        """
        Returns an item from the database
        """

        key = {cls._config.indexes.table.partitionkey: pk}

        if cls._config.indexes.table.sortkey:
            key[cls._config.indexes.table.sortkey] = sk

        serialized_key = {
            k: TypeSerializer().serialize(v)
            for k, v in key.items()
        }

        opts = {
            "Key": serialized_key,
            "TableName": cls._config.table,
            **kwargs
        }

        res = DYNAMOCLIENT.get_item(**opts)

        if item := res.get("Item"):
            res["Item"] = cls.construct(**cls.deserialize_dynamo(item))

        return item

    def set_compound_attribute(cls, name: str) -> None:

        for target, attributes in cls._config.joined_attributes.items():
            if name in attributes:
                val = cls._config.join_separator.join(
                    [str(getattr(cls, attribute, "")) for attribute in attributes]
                )
                cls.__setattr__(target, val)

    @classmethod
    def _get_index_from_attributes(cls, partitionkey: str = None, sortkey: str = None) -> Index:
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
                partitionkey is not None and sortkey is not None
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
    def _get_raw_condition_expression(self, exp: ConditionBase, index: Union[Index, str] = None, expression_type="KeyConditionExpression"):
        is_key_condition = expression_type == "KeyConditionExpression"
        raw_exp = ConditionExpressionBuilder().build_expression(exp, is_key_condition=is_key_condition)


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
                    raw_exp.attribute_name_placeholders[placeholder] = index.partitionkey
                if len(attribute_names) == 2 and attr == attribute_names[1]:
                    raw_exp.attribute_name_placeholders[placeholder] = index.sortkey


        for k, v in raw_exp.attribute_value_placeholders.items():
            raw_exp.attribute_value_placeholders[k] = TypeSerializer().serialize(v)

        opts = {}

        if expression_type == "KeyConditionExpression":
            print(raw_exp.attribute_name_placeholders.values())
            opts["IndexName"] = index.name
            opts["KeyConditionExpression"] = raw_exp.condition_expression
            opts["ExpressionAttributeNames"] = raw_exp.attribute_name_placeholders

        elif expression_type == "FilterExpression":
            opts["FilterExpression"] = raw_exp.condition_expression

        else:
            raise TypeError("Invalid Condition type. Must be one of KeyConditionExpression, or FilterExpression")

        opts["ExpressionAttributeValues"] = raw_exp.attribute_value_placeholders
        opts["TableName"] = self._config.table


        return opts


    def save(self) -> None:
        """
        Stores our item in Dynamodb
        """

        item = {
            k: TypeSerializer(v)
            for k, g in self.item.item()
        }

        return DYNAMOCLIENT.put_item(Item=item)

    @classmethod
    def get_index_by_name(cls, name: str) -> Index:
        try:
            return cls._config.indexes[name]
        except KeyError:
            raise IndexNotFoundError(f"Index {name} does not exist")

    @classmethod
    def query(
        cls,
        KeyConditionExpression: ConditionBase,
        Index: Union[Index, str] = None,
        FilterExpression: AttributeBase = None,
        Limit: int = 1000,
        ExclusiveStartKey: dict = None,
        **kwargs
    ) -> Dict:
        """
        Runs a Dynamodb query using a condition from db.Inde x
        """

        opts = {
            **kwargs,
            "Limit": Limit
        }

        opts.update(cls._get_raw_condition_expression(
            exp=KeyConditionExpression,
            index=Index
        ))


        if FilterExpression is not None:
            opts.update(cls._get_raw_condition_expression(
                exp=FilterExpression,
                expression_type="FilterExpression"
            ))

        if ExclusiveStartKey is not None:
            opts["ExclusiveStartKey"] = ExclusiveStartKey

        getLogger().info(f"Querying with index `{opts['IndexName']}`")

        res = DYNAMOCLIENT.query(**opts)

        res["Items"] = [
            cls(**cls.deserialize_dynamo(item))
            for item in res["Items"]
        ]

        return res


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
    def deserialize_dynamo(data):
        return {
            k: TypeDeserializer().deserialize(v)
            for k, v in data.items()
        }
