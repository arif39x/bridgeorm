from contextvars import ContextVar
from typing import Any, Dict, List, Optional, Type, get_type_hints

import bridge_orm_rs

from ..common import HookAbortedError, NotFoundError
from .hooks import dispatch_hooks
from .query import QueryBuilder

BULK_INSERT_CHUNK_SIZE = 1000

# Scoped to the current asyncio Task
# Maps (table_name, primary_key) -> Model Instance
_IDENTITY_MAP: ContextVar[Optional[Dict[tuple, Any]]] = ContextVar(
    "identity_map", default=None
)

_MODEL_REGISTRY: Dict[str, Type["BaseModel"]] = {}


class BaseModel:
    table: str = ""
    _fields: List[str] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.table:
            _MODEL_REGISTRY[cls.table] = cls

    @classmethod
    def _get_hooks(cls):
        if "_hooks" not in cls.__dict__:
            cls._hooks = {}
        return cls._hooks

    @classmethod
    def get_field_definitions(cls) -> Dict[str, str]:
        """Extract field types using Python type hints for the migration engine."""
        hints = get_type_hints(cls)
        defs = {}
        # Filter hints to only include fields in _fields
        for field in cls._fields:
            type_obj = hints.get(field, Any)
            # Simple string representation of type for the snapshot
            type_name = getattr(type_obj, "__name__", str(type_obj))
            if "Optional" in str(type_obj):
                type_name = f"Optional[{type_name}]"
            defs[field] = type_name
        return defs

    @classmethod
    def query(cls) -> QueryBuilder:
        return QueryBuilder(cls)

    @classmethod
    def _get_identity_cache(cls) -> Dict[tuple, Any]:
        """Retrieve the identity map for the current task context."""
        import asyncio

        current_task = asyncio.current_task()
        if not hasattr(current_task, "_bridge_orm_identity_map"):
            setattr(current_task, "_bridge_orm_identity_map", {})
        return getattr(current_task, "_bridge_orm_identity_map")

    @classmethod
    async def create(cls, tx=None, **kwargs) -> Any:
        # Create instance locally for hooks
        class TempInstance:
            pass

        instance = TempInstance()
        for k, v in kwargs.items():
            setattr(instance, k, v)

        await dispatch_hooks(cls, "before_create", instance)

        # Convert kwargs to string map for generic Rust interface
        data = {k: str(v) for k, v in kwargs.items()}

        # Add default values if missing (id, created_at, updated_at)
        if "id" not in data:
            import uuid

            data["id"] = str(uuid.uuid4())
        if "created_at" not in data:
            from datetime import datetime

            data["created_at"] = datetime.now().isoformat()
        if "updated_at" not in data:
            from datetime import datetime

            data["updated_at"] = datetime.now().isoformat()

        # Call generic Rust insert
        raw_res = await bridge_orm_rs.insert_row(cls.table, data, tx=tx)

        # Map back to model instance
        res = cls(**raw_res)

        # Populate Identity Map
        cache = cls._get_identity_cache()
        cache[(cls.table, str(res.id))] = res

        await dispatch_hooks(cls, "after_create", res)
        return res

    @classmethod
    async def create_many(cls, items: List[Dict[str, Any]], tx=None) -> List[Any]:
        """Insert multiple records in batches to optimize FFI crossings."""
        results = []
        for i in range(0, len(items), BULK_INSERT_CHUNK_SIZE):
            chunk = items[i : i + BULK_INSERT_CHUNK_SIZE]
            # Prep data with defaults for each item in chunk
            prepared_chunk = []
            for item in chunk:
                data = {k: str(v) for k, v in item.items()}
                if "id" not in data:
                    import uuid

                    data["id"] = str(uuid.uuid4())
                if "created_at" not in data:
                    from datetime import datetime

                    data["created_at"] = datetime.now().isoformat()
                if "updated_at" not in data:
                    from datetime import datetime

                    data["updated_at"] = datetime.now().isoformat()
                prepared_chunk.append(data)

            # Single FFI crossing for the entire chunk
            raw_results = await bridge_orm_rs.insert_rows_bulk(
                cls.table, prepared_chunk, tx=tx
            )

            # Map back to model instances and populate Identity Map
            cache = cls._get_identity_cache()
            for raw_res in raw_results:
                instance = cls(**raw_res)
                cache[(cls.table, str(instance.id))] = instance
                results.append(instance)

        return results

    @classmethod
    async def find_one(cls, tx=None, **kwargs) -> Optional[Any]:
        # Consultation: Check Identity Map first if searching by ID
        if "id" in kwargs:
            cache = cls._get_identity_cache()
            if (cls.table, str(kwargs["id"])) in cache:
                return cache[(cls.table, str(kwargs["id"]))]

        # Convert filters to string map
        filters = {k: str(v) for k, v in kwargs.items()}

        raw_res = await bridge_orm_rs.find_one(cls.table, filters)
        if raw_res is None:
            raise NotFoundError(f"No {cls.__name__} found matching {kwargs}")

        res = cls(**raw_res)

        # Population: Add to cache on miss
        cache = cls._get_identity_cache()
        cache[(cls.table, str(res.id))] = res

        return res

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_dict(self) -> Dict[str, Any]:
        return {f: getattr(self, f, None) for f in self._fields}
