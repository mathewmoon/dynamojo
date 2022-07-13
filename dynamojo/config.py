#!/usr/bin/env python3
from typing import Union, Callable, List, Dict

from pydantic import BaseModel, PrivateAttr

from .index import IndexList, IndexMap, Mutator


class DynamojoConfig(BaseModel):
    # A dictionary in the form of {"<target attribute>": ["source_att_one", "source_att_two"]} where <target_attribute> will
    # automatically be overwritten by the attributes of it's corresponding list being joined with '~'. This is useful for creating
    # keys that can be queried over using Key().begins_with() or Key().between() by creating a means to filter based on the compounded
    # attributes.
    joined_attributes: Dict[str, Union[List[str], Callable]] = {}

    # A list of database `Index` objects from `dynamojo.indexes.get_indexes()``
    indexes: IndexList

    # A list of `IndexMap` objects used to map arbitrary fields into index attributes
    index_maps: List[IndexMap] = []

    static_attributes: List[str] = []

    # A Dynamodb table name
    table: str

    join_separator: str = "~"

    mutators: List[Mutator] = []

    joined_attributes: Dict[str, List[str]] = {}

    static_attributes: List[str] = []

    # If set to False then attributes that are aliases of indexes will be stripped
    # out before storing in the db
    store_aliases: bool = True

    underscore_attrs_are_private: bool = True

    # Dict of `index key: alias name`
    __index_aliases__: dict = PrivateAttr(default={})

    __index_keys__: List[str] = PrivateAttr(default={})

    class Config:
        underscore_attrs_are_private: bool = True
        arbitrary_types_allowed = True
        extra = "allow"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for index_map in self.index_maps:
            if sk_att := index_map.sortkey:
                self.__index_aliases__[index_map.index.sortkey] = sk_att
            if getattr(index_map, "partitionkey", None):
                self.__index_aliases__[
                    index_map.index.partitionkey
                ] = index_map.partitionkey

        self.__index_keys__ = list(set(self.__index_aliases__.keys()))
