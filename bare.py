#!/usr/bin/env python3.8
from pydantic import BaseModel, PrivateAttr


class FooBase(BaseModel):
    class Config:
        underscore_attributes_are_private = True

    _config: dict = PrivateAttr()
    _alias_fields: dict = PrivateAttr({})
    _initialized: bool = PrivateAttr(False)

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        for attr, alias in self._config.items():
            self.__fields__[alias] = self.__fields__[attr]
            self.__annotations__[alias] = self.__annotations__[attr]

            super().__setattr__(alias, self.__getattribute__(attr))

        self._initialized = True

        # for k, v in self.dict().items():
        #    self.__setattr__(k, v)

    def __setattr__(self, key, val):
        if self._initialized and key in self._config:
            alias = self._config[key]
            super().__setattr__(alias, val)
        return super().__setattr__(key, val)


class Foo(FooBase):
    bar: str
    baz: str
    _config: dict = {"bar": "bar_alias", "baz": "baz_alias"}


test = Foo(bar="bar", baz="baz")
print(test.dict())
test.bar = "changed"
print(test.bar_alias)
print(test)
