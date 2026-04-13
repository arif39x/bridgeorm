from typing import Dict, Any, List, Optional, Type
from ..common import QueryError
import bridge_orm_rs

class QueryBuilder:
    def __init__(self, model_class: Type['BaseModel']):
        self.model_class = model_class
        self._filters: Dict[str, Any] = {}
        self._limit: Optional[int] = None

    def filter(self, **kwargs) -> 'QueryBuilder':
        self._filters.update(kwargs)
        return self

    def limit(self, count: int) -> 'QueryBuilder':
        self._limit = count
        return self

    async def fetch(self) -> List[Any]:
        # Call the generic Rust fetch
        filters = {k: str(v) for k, v in self._filters.items()}
        raw_results = await bridge_orm_rs.fetch_all(
            self.model_class.table, 
            filters, 
            self._limit
        )
        return [self.model_class(**res) for res in raw_results]

    def fetch_lazy(self) -> Any:
        """Return an async iterator for the query results."""
        filters = {k: str(v) for k, v in self._filters.items()}
        stream = bridge_orm_rs.fetch_lazy(
            self.model_class.table, 
            filters, 
            self._limit
        )
        
        async def mapper():
            async for item in stream:
                yield self.model_class(**item)
        
        return mapper()

    async def first(self) -> Optional[Any]:
        res = await self.limit(1).fetch()
        return res[0] if res else None
