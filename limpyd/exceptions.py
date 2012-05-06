__all__ = [
    "UniquenessError",
    "ImplementationError",
    "DoesNotExist",
]


class LimpydException(Exception):
    pass


class UniquenessError(LimpydException):
    pass


class DoesNotExist(LimpydException):
    pass


class ImplementationError(LimpydException):
    pass
