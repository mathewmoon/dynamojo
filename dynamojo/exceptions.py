class DynamodbException(Exception):
  pass


class RequiredAttributeError(DynamodbException):
  pass


class StaticAttributeError(DynamodbException):
  pass


class UnknownAttributeError(DynamodbException):
  pass


class ProtectedAttributeError(DynamodbException):
  pass


class ItemNotFoundError(DynamodbException):
  pass


class NotAuthorized(DynamodbException):
  pass


class IndexNotFoundError(DynamodbException):
  pass
