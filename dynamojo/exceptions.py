class RbacManagerException(Exception):
  pass


class RequiredAttributeError(RbacManagerException):
  pass


class StaticAttributeError(RbacManagerException):
  pass


class UnknownAttributeError(RbacManagerException):
  pass


class ProtectedAttributeError(RbacManagerException):
  pass


class ItemNotFoundError(RbacManagerException):
  pass


class NotAuthorized(RbacManagerException):
  pass
