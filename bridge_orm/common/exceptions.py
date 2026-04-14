class BridgeORMError(Exception):
    """Base exception for all BridgeORM errors."""
    pass

class NotFoundError(BridgeORMError, KeyError):
    """Raised when a requested resource is not found in the database."""
    pass

class ValidationError(BridgeORMError, ValueError):
    """Raised when data fails validation before database interaction."""
    pass

class HookAbortedError(BridgeORMError):
    """Raised when a pre-save/delete hook aborts the operation."""
    pass

class DatabaseError(BridgeORMError):
    """Raised when the database engine returns an error."""
    pass
