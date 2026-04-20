from typing import Any, AsyncIterator, Dict, List, Optional, Type

import bridge_orm_rs

from ..common.exceptions import DatabaseError, ValidationError


class Raw:
    """Wrapper for raw SQL expressions with bound parameters."""
    __slots__ = ("sql", "params")

    def __init__(self, sql: str, *params: Any) -> None:
        self.sql = sql
        self.params = list(params)


class QueryBuilder:
    # Fluent interface for building and executing database queries.
    # Rule: Use __slots__ on hot-path classes.

    __slots__ = ("model_class", "_filters", "_limit", "_projection")

    def __init__(self, model_class: Type["BaseModel"]) -> None:

        # Initialize the QueryBuilder.
        # Args:
        #   model_class: The model class to query.

        self.model_class = model_class
        self._filters: Dict[str, Any] = {}
        self._limit: Optional[int] = None
        self._projection: Optional[List[str]] = None

    def select(self, *fields: str) -> "QueryBuilder":

        # Restrict the SQL projection to the specified columns.
        # Args:
        #    *fields: Column names to select.
        # Returns:
        #    The QueryBuilder instance for chaining.

        self._projection = list(fields)
        return self

    def filter(self, **kwargs: Any) -> "QueryBuilder":

        # Add filters to the query.
        # Args:
        #    **kwargs: Column names and values to filter by.

        # Returns:
        #   The QueryBuilder instance for chaining.
        #
        self._filters.update(kwargs)
        return self

    def limit(self, count: int) -> "QueryBuilder":
        """
        Limit the number of results returned.

        Args:
            count: Maximum number of rows to return.

        Returns:
            The QueryBuilder instance for chaining.
        """

        if count < 0:
            raise ValidationError("Limit count must be non-negative")
        self._limit = count
        return self

    async def fetch(self, tx: Any = None) -> List[Any]:
        """
        Execute the query and return all results as model instances.

        Returns:
            A list of model instances.

        Raises:
            DatabaseError: If the database engine returns an error.
        """
        filters = self._filters
        try:
            # Handle Session or TxHandle
            rs_tx = tx._rs_session if hasattr(tx, "_rs_session") else tx
            raw_results = await bridge_orm_rs.fetch_all(
                self.model_class.table, filters, self._limit, self._projection, tx=rs_tx
            )
            instances = []
            for res in raw_results:
                instance = self.model_class(**res)
                if hasattr(tx, "set_entity"):
                    instance._session = tx
                if self._projection:
                    instance._projected_fields = self._projection
                
                # Identity Map population
                if hasattr(tx, "set_entity") and not self._projection:
                    pk_values = tuple(getattr(instance, k) for k in self.model_class._primary_keys)
                    tx.set_entity(self.model_class, pk_values, instance)
                
                instances.append(instance)
            return instances
        except Exception as e:
            raise DatabaseError(f"Fetch failed: {e}") from e

    async def fetch_lazy(self, tx: Any = None) -> AsyncIterator[Any]:
        """
        Execute the query and return an async iterator for the results.

        Returns:
            An async iterator of model instances.
        """
        filters = self._filters
        # Handle Session or TxHandle
        rs_tx = tx._rs_session if hasattr(tx, "_rs_session") else tx
        stream = bridge_orm_rs.fetch_lazy(
            self.model_class.table, filters, self._limit, self._projection, tx=rs_tx
        )

        async for item in stream:
            instance = self.model_class(**item)
            if hasattr(tx, "set_entity"):
                instance._session = tx
            if self._projection:
                instance._projected_fields = self._projection
            
            # Identity Map population
            if hasattr(tx, "set_entity") and not self._projection:
                pk_values = tuple(getattr(instance, k) for k in self.model_class._primary_keys)
                tx.set_entity(self.model_class, pk_values, instance)

            yield instance

    async def first(self, tx: Any = None) -> Optional[Any]:
        """
        Execute the query and return the first result, or None if no results.

        Returns:
            A model instance or None.
        """
        res = await self.limit(1).fetch(tx=tx)
        return res[0] if res else None
