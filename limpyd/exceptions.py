__all__ = [
    "UniquenessError",
    "ImplementationError"
]

class LimpydException(Exception):
    pass

class UniquenessError(LimpydException):
    pass

class ImplementationError(LimpydException):
    pass
