from typing import (
  List,
  Dict,
  TYPE_CHECKING
)

from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel

from .index import (
  Index,
  IndexMap
)

if not TYPE_CHECKING:
  Table = object

class DynamojoConfig(BaseModel):
  _all_attributes = None

  class Config:
    arbitrary_types_allowed = True

  indexes: Dict[str, Index]
  index_maps: List[IndexMap]
  table: Table
  compound_attributes: Dict[str, List[str]] = {}
  protected_attributes: List[str] = []
  required_attributes: List[str] = []
  static_attributes: List[str] = []
 