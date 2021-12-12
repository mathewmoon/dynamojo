from typing import NamedTuple
from . import base

class AppUser(NamedTuple):
  username: str
  is_admin: bool = False


def set_caller(username: str, is_admin=False) -> AppUser:
  base.ObjectBase.Caller = AppUser(
    username=username,
    is_admin=is_admin
  )
