#!/usr/bin/env python3
from typing import List, Dict

import boto3
from pydantic import BaseModel, PrivateAttr, Field

from .index import IndexList, IndexMap, Mutator


class JoinedAttribute(BaseModel):
    attribute: str
    fields: List[str]
    separator: str = "~"


class DynamojoConfig(BaseModel):
    # A dictionary in the form of {"<target attribute>": ["source_att_one", "source_att_two"]} where <target_attribute> will
    # automatically be overwritten by the attributes of it's corresponding list being joined with '~'. This is useful for creating
    # keys that can be queried over using Key().begins_with() or Key().between() by creating a means to filter based on the compounded
    # attributes.
    convert_dynamodb_types: bool = Field(default=True)
    joined_attributes: List[JoinedAttribute]
    __joined_attributes__: Dict = {}

    # A list of database `Index` objects from `dynamojo.indexes.get_indexes()``
    indexes: IndexList

    # A list of `IndexMap` objects used to map arbitrary fields into index attributes
    index_maps: List[IndexMap] = Field(default_factory=list)

    static_attributes: List[str] = Field(default_factory=str)

    # A Dynamodb table name
    table: str

    dynamo_client: object = Field(default_factory=lambda: boto3.client("dynamodb"))

    mutators: List[Mutator] = Field(default_factory=list)

    # If set to False then attributes that are aliases of indexes will be stripped
    # out before storing in the db
    store_aliases: bool = True

    # Dict of `index key: alias name`
    _index_aliases: dict = PrivateAttr(default={})

    _index_keys: List[str] = PrivateAttr(default={})

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for attr in self.joined_attributes:
            self.__joined_attributes__[attr.attribute] = attr

        for index_map in self.index_maps:
            if sk_att := index_map.sortkey:
                self._index_aliases[index_map.index.sortkey] = sk_att
            if getattr(index_map, "partitionkey", None):
                self._index_aliases[index_map.index.partitionkey] = (
                    index_map.partitionkey
                )

        self._index_keys = list(set(self._index_aliases.keys()))
