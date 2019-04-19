import Exceptions


class ProtoBaseException(Exceptions):
    pass


class ValidationException(ProtoBaseException):
    pass


class UserException(ProtoBaseException):
    pass


class CorruptionException(ProtoBaseException):
    pass


class NotImplemented(ProtoBaseException):
    pass

