from typing import Any, AsyncIterator, Dict, List, Optional, Type

import bridge_orm_rs

from ..common.exceptions import DatabaseError, ValidationError


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

    async def fetch(self) -> List[Any]:
        """
        Execute the query and return all results as model instances.

        Returns:
            A list of model instances.

        Raises:
            DatabaseError: If the database engine returns an error.
        """
        filters = {k: str(v) for k, v in self._filters.items()}
        try:
            raw_results = await bridge_orm_rs.fetch_all(
                self.model_class.table, filters, self._limit, self._projection
            )
            instances = []
            for res in raw_results:
                instance = self.model_class(**res)
                if self._projection:
                    instance._projected_fields = self._projection
                instances.append(instance)
            return instances
        except Exception as e:
            raise DatabaseError(f"Fetch failed: {e}") from e

    async def fetch_lazy(self) -> AsyncIterator[Any]:
        """
        Execute the query and return an async iterator for the results.

        Returns:
            An async iterator of model instances.
        """
        filters = {k: str(v) for k, v in self._filters.items()}
        stream = bridge_orm_rs.fetch_lazy(
            self.model_class.table, filters, self._limit, self._projection
        )

        async for item in stream:
            instance = self.model_class(**item)
            if self._projection:
                instance._projected_fields = self._projection
            yield instance

    async def first(self) -> Optional[Any]:
        """
        Execute the query and return the first result, or None if no results.

        Returns:
            A model instance or None.
        """
        res = await self.limit(1).fetch()
        return res[0] if res else None
