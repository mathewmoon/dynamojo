#!/usr/bin/env python3.8
from logging import getLogger
from typing import (
    Any,
    Dict,
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
from pydantic import (
    BaseModel,
    PrivateAttr
)
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


    def __init__(self, **kwargs: Dict[str, Any]) -> None:

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
                kwargs[k] = self.mutate_attribute(k, v)


    def __getattribute__(self, name: str) -> Any:
        if super().__getattribute__("_config").joined_attributes.get(name):
            return self._generate_joined_attribute(name)

        return super().__getattribute__(name)


    def __setattr__(self, field: str, val: Any) -> None:

        # Mutations should happen first
        if field in self._config.mutators:
            val = self.mutate_attribute(field, val)

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
            and hasattr(self, field)
            and self.__getattribute__(field) != val
        ):
            raise StaticAttributeError(f"Attribute '{field}' is immutable.")

        return super().__setattr__(field, val)


    def _db_item(self) -> dict:
        return {
            **self.dict(),
            **self.joined_attributes(),
            **self.index_attributes()
        }


    def _generate_joined_attribute(self, name: str) -> str:
        item = super().__getattribute__("dict")()
        sources = super().__getattribute__("_config").joined_attributes.get(name)
        new_val = [
            item.get(source, "") for source in sources
        ]
        return self._config.join_separator.join(new_val)


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
        raw_exp = ConditionExpressionBuilder().build_expression(
            exp, is_key_condition=is_key_condition)

        if is_key_condition:
            attribute_names = list(
                raw_exp.attribute_name_placeholders.values())
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
            raw_exp.attribute_value_placeholders[k] = TypeSerializer(
            ).serialize(v)

        opts = {}

        if expression_type == "KeyConditionExpression":
            if index.name != "table":
                opts["IndexName"] = index.name
            opts["KeyConditionExpression"] = raw_exp.condition_expression
            opts["ExpressionAttributeNames"] = raw_exp.attribute_name_placeholders

        elif expression_type == "FilterExpression":
            opts["FilterExpression"] = raw_exp.condition_expression

        else:
            raise TypeError(
                "Invalid Condition type. Must be one of KeyConditionExpression, or FilterExpression")

        opts["ExpressionAttributeValues"] = raw_exp.attribute_value_placeholders
        opts["TableName"] = self._config.table

        return opts


    @classmethod
    def construct_from_db(cls, item):
        item = cls.deserialize_dynamo(item)
        res = {}
        for attr, val in item.items():
            if not (
                attr in cls._config.__index_keys__
                or attr in cls._config.joined_attributes
            ):
                res[attr] = val

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
    def deserialize_dynamo(data):
        return {
            k: TypeDeserializer().deserialize(v)
            for k, v in data.items()
        }

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
            return cls.construct_from_db(item)


    @classmethod
    def get_index_by_name(cls, name: str) -> Index:
        try:
            return cls._config.indexes[name]
        except KeyError:
            raise IndexNotFoundError(f"Index {name} does not exist")


    def index_attributes(self) -> dict:
        indexes = {}
        for mapping in self._config.index_maps:
            if hasattr(mapping, "partitionkey"):
                indexes[mapping.index.partitionkey] = self.__getattribute__(mapping.partitionkey)
            if hasattr(mapping, "sortkey"):
                indexes[mapping.index.sortkey] = self.__getattribute__(mapping.sortkey)
        return indexes


    def item(self) -> dict:
        return {
            **self.dict(),
            **self.joined_attributes()
        }


    def joined_attributes(self) -> dict:
        return {
            attr: self.__getattribute__(attr)
            for attr in self._config.joined_attributes
        }


    def mutate_attribute(cls, field: str, val: Any) -> None:
        return super().__setattr__(
            field, cls._config.mutators[field].callable(field, val, cls)
        )


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

        msg = f"Querying with index `{opts['IndexName']}`" if opts.get(
            "IndexName") else "Querying with table index"
        getLogger().info(msg)

        res = DYNAMOCLIENT.query(**opts)

        res["Items"] = [
            cls(**cls.deserialize_dynamo(item))
            for item in res["Items"]
        ]

        return res


    def save(self) -> None:
        """
        Stores our item in Dynamodb
        """
        item = {
            k: TypeSerializer().serialize(v)
            for k, v in self._db_item().items()
        }

        return DYNAMOCLIENT.put_item(
            TableName=self._config.table,
            Item=item
        )
