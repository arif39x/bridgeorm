class BridgeORMError(Exception):
    """Base exception for all BridgeORM errors."""
    pass

class ConnectionError(BridgeORMError):
    """Raised when the database connection pool is not initialized or fails."""
    pass

class QueryError(BridgeORMError):
    """Raised when a query is invalid, e.g., unknown field in filter."""
    pass

class NotFoundError(BridgeORMError):
    """Raised when find_one fails to locate a record."""
    pass

class ConstraintError(BridgeORMError):
    """Raised when a database constraint (like unique or foreign key) is violated."""
    pass

class HookAbortedError(BridgeORMError):
    """Raised when a Before* hook returns False and cancels the operation."""
    pass
