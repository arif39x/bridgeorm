class BridgeORMError(Exception):
    """Base exception for all BridgeORM errors."""
    pass

class ConnectionError(BridgeORMError):
    """Raised when database connection fails."""
    pass

class QueryError(BridgeORMError):
    """Raised when a database query fails."""
    pass

class NotFoundError(BridgeORMError, KeyError):
    """Raised when a requested resource is not found in the database."""
    pass

class ConstraintError(BridgeORMError):
    """Raised when a database constraint is violated."""
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

class ProjectionError(BridgeORMError, AttributeError):
    """Raised when accessing an unselected field in a partial model."""
    pass

class CompositeKeyError(BridgeORMError):
    """Raised when composite primary key operations fail."""
    pass
