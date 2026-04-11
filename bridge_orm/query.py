from typing import Dict, Any, List, Optional, Type
from .exceptions import QueryError
import bridge_orm_rs

class QueryBuilder:
    def __init__(self, model_class: Type['BaseModel']):
        self.model_class = model_class
        self._filters: Dict[str, str] = {}
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    def filter(self, **kwargs) -> 'QueryBuilder':
        for key, value in kwargs.items():
            if key not in self.model_class._fields:
                raise QueryError(f"'{key}' is not a valid field for {self.model_class.__name__}")
            self._filters[key] = str(value)
        return self

    def limit(self, n: int) -> 'QueryBuilder':
        self._limit = n
        return self

    def eager(self, **kwargs) -> 'QueryBuilder':
        """Eagerly load related models."""
        for name, model_class in kwargs.items():
            # In a real ORM, we'd store these for the fetch operation.
            pass
        return self

    def lazy(self, **kwargs) -> 'QueryBuilder':
        """Defer loading of related models."""
        return self

    def offset(self, n: int) -> 'QueryBuilder':
        self._offset = n
        return self

    async def fetch(self) -> List[Any]:
        # In a real implementation, we would call a more generic query function
        # For this prototype, we're focusing on User
        if self.model_class.table == "users":
            return bridge_orm_rs.query_users(self._filters, self._limit)
        return []
