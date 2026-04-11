import bridge_orm_rs
from .base import BaseModel
from .exceptions import BridgeORMError, ConnectionError, QueryError, NotFoundError, ConstraintError, HookAbortedError
from .transaction import transaction

async def connect(url: str):
    """Initialise the database connection pool."""
    return bridge_orm_rs.connect(url)

def configure_logging(level: str = "info", slow_query_ms: int = 100):
    """Configure structured query logging."""
    bridge_orm_rs.configure_logging(level, slow_query_ms)

# Re-expose model classes with metadata
class User(BaseModel):
    table = "users"
    _fields = ["id", "username", "email", "created_at", "updated_at"]

    async def load_related(self, model_class):
        if model_class.__name__ == "Post":
            return bridge_orm_rs.load_related_posts(str(self.id))
        return []

class Post(BaseModel):
    table = "posts"
    _fields = ["id", "title", "user_id", "created_at", "updated_at"]
