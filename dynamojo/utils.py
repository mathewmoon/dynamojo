#!/usr/bin/env python3
from pydantic import BaseModel, PrivateAttr
from typing import TYPE_CHECKING, Any, Dict, NamedTuple


if TYPE_CHECKING:
    from .base import DynamojoBase
else:
    DynamojoBase = object

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


TYPE_SERIALIZER = TypeSerializer()
TYPE_DESERIALIZER = TypeDeserializer()

Change = NamedTuple("Change", [("old", Any), ("new", Any)])
Diff = NamedTuple(
    "Diff",
    (
        ("added", Dict[str, Any]),
        ("removed", Dict[str, Any]),
        ("changed", Dict[str, Any]),
    ),
)


class Delta(BaseModel):
    old: DynamojoBase = None
    new: DynamojoBase = None

    _deep: bool = PrivateAttr()
    _old: Dict[str, Any] = PrivateAttr()
    _new: Dict[str, Any] = PrivateAttr()

    def __init__(self, deep=True, **kwargs):
        self._deep = deep
        super().__init__(**kwargs)

        if deep:
            self._old = self.old._db_item()
            self._new = self.new._db_item()
        else:
            self._old = self.old.item()
            self._new = self.new.item()

    @property
    def delta(self) -> Diff:
        diff = Diff({}, {}, {})

        for key, val in self._old.items():
            if key not in self._new:
                diff.removed[key] = val
            if key in self._new and val != self._new[key]:
                diff.changed[key] = Change(self._old[key], self._new[key])

        for key, val in self._new.items():
            if key not in self._old:
                diff.added[key] = val

        return diff

    @property
    def added(self) -> Dict[str, Any]:
        return self.delta.added

    @property
    def changed(self) -> Change:
        return self.delta.changed

    @property
    def removed(self) -> Dict[str, Any]:
        return self.delta.removed

    @property
    def hasChanged(self):
        return not self._old == self._new

    @property
    def keys(self):
        return {**self.added, **self.removed, **self.changed}.keys()
