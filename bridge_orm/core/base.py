from contextvars import ContextVar
from typing import Any, Dict, List, Optional, Type, get_type_hints

import bridge_orm_rs

from ..common import CompositeKeyError, HookAbortedError, NotFoundError, ProjectionError
from .hooks import dispatch_hooks
from .query import QueryBuilder

BULK_INSERT_CHUNK_SIZE = 1000

# Scoped to the current asyncio Task
# Maps (table_name, primary_key_tuple) -> Model Instance
_IDENTITY_MAP: ContextVar[Optional[Dict[tuple, Any]]] = ContextVar(
    "identity_map", default=None
)

_MODEL_REGISTRY: Dict[str, Type["BaseModel"]] = {}


class BaseModel:
    table: str = ""
    _fields: List[str] = []
    _primary_keys: List[str] = ["id"]
    _projected_fields: Optional[List[str]] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.table:
            _MODEL_REGISTRY[cls.table] = cls

    @classmethod
    def _build_pk_predicate(cls, **kwargs) -> Dict[str, Any]:
        # Construct a predicate dictionary for primary key lookup
        missing = [k for k in cls._primary_keys if k not in kwargs]
        if missing:
            raise CompositeKeyError(
                f"Model '{cls.__name__}' requires all primary key fields. "
                f"Missing: {missing}. Required: {cls._primary_keys}"
            )
        return {k: kwargs[k] for k in cls._primary_keys}

    @classmethod
    def _build_cache_key(cls, **kwargs) -> tuple:
        # Construct a deterministic cache key from primary key values
        predicate = cls._build_pk_predicate(**kwargs)
        # Sort by key name to ensure deterministic tuple
        return (cls.table, tuple(sorted(predicate.items())))

    @classmethod
    def _get_hooks(cls):
        if "_hooks" not in cls.__dict__:
            cls._hooks = {}
        return cls._hooks

    @classmethod
    def get_field_definitions(cls) -> Dict[str, str]:
        # Extract field types using Python type hints for the migration engine
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
        # Retrieve the identity map for the current task context
        import asyncio

        current_task = asyncio.current_task()
        if current_task is None:
            return {}
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

        # Add default values if missing
        if "id" in cls._primary_keys and "id" not in data:
            import uuid

            data["id"] = str(uuid.uuid4())

        if "created_at" in cls._fields and "created_at" not in data:
            from datetime import datetime

            data["created_at"] = datetime.now().isoformat()

        if "updated_at" in cls._fields and "updated_at" not in data:
            from datetime import datetime

            data["updated_at"] = datetime.now().isoformat()

        # Call generic Rust insert
        raw_res = await bridge_orm_rs.insert_row(cls.table, data, tx=tx)

        # Map back to model instance
        res = cls(**raw_res)

        # Populate Identity Map
        cache = cls._get_identity_cache()
        pk_values = {k: getattr(res, k) for k in cls._primary_keys}
        cache[cls._build_cache_key(**pk_values)] = res

        await dispatch_hooks(cls, "after_create", res)
        return res

    @classmethod
    async def create_many(cls, items: List[Dict[str, Any]], tx=None) -> List[Any]:
        # Insert mulltiple records in batches to optimize FFI crossings.
        results = []
        for i in range(0, len(items), BULK_INSERT_CHUNK_SIZE):
            chunk = items[i : i + BULK_INSERT_CHUNK_SIZE]
            # Prep data with defaults for each item in chunk
            prepared_chunk = []
            for item in chunk:
                data = {k: str(v) for k, v in item.items()}
                if "id" in cls._primary_keys and "id" not in data:
                    import uuid

                    data["id"] = str(uuid.uuid4())

                if "created_at" in cls._fields and "created_at" not in data:
                    from datetime import datetime

                    data["created_at"] = datetime.now().isoformat()

                if "updated_at" in cls._fields and "updated_at" not in data:
                    from datetime import datetime

                    data["updated_at"] = datetime.now().isoformat()
                prepared_chunk.append(data)

            # Single FFI crossiing for the entire chunk
            raw_results = await bridge_orm_rs.insert_rows_bulk(
                cls.table, prepared_chunk, tx=tx
            )

            # Map back to model instances and populate Identity Map
            cache = cls._get_identity_cache()
            for raw_res in raw_results:
                instance = cls(**raw_res)
                pk_values = {k: getattr(instance, k) for k in cls._primary_keys}
                cache[cls._build_cache_key(**pk_values)] = instance
                results.append(instance)

        return results

    @classmethod
    async def find_one(
        cls, tx=None, fields: Optional[List[str]] = None, **kwargs
    ) -> Optional[Any]:
        #  Check Identity Map first if all primary keys provided
        pk_provided = all(k in kwargs for k in cls._primary_keys)

        # Enforcement: If it's a composite key, don't allow 'id' unless it's actually a PK
        if (
            "id" in kwargs
            and "id" not in cls._primary_keys
            and len(cls._primary_keys) > 1
        ):
            raise CompositeKeyError(
                f"Model '{cls.__name__}' has a composite key {cls._primary_keys}. "
                f"Cannot use 'id' for lookup."
            )

        # Enforce full PK if doing a PK-like lookup
        # consider it a PK lookup if ANY of the PKs are in kwargs
        any_pk_provided = any(k in kwargs for k in cls._primary_keys)
        if any_pk_provided and not pk_provided:
            raise CompositeKeyError(
                f"Model '{cls.__name__}' requires ALL primary key fields for PK lookup. "
                f"Missing: {[k for k in cls._primary_keys if k not in kwargs]}"
            )

        if pk_provided and fields is None:
            cache = cls._get_identity_cache()
            cache_key = cls._build_cache_key(**kwargs)
            if cache_key in cache:
                return cache[cache_key]

        # Convert filters to string map
        filters = {k: str(v) for k, v in kwargs.items()}

        raw_res = await bridge_orm_rs.find_one(cls.table, filters, fields=fields)
        if raw_res is None:
            raise NotFoundError(f"No {cls.__name__} found matching {kwargs}")

        res = cls(**raw_res)
        if fields:
            res._projected_fields = fields

        # Add to cache on miss (only if full model)
        if fields is None:
            cache = cls._get_identity_cache()
            pk_values = {k: getattr(res, k) for k in cls._primary_keys}
            cache[cls._build_cache_key(**pk_values)] = res

        return res

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getattr__(self, name: str) -> Any:
        if self._projected_fields is not None and name in self._fields:
            if name not in self._projected_fields:
                raise ProjectionError(
                    f"Field '{name}' was not included in the SELECT projection. "
                    f"Projected fields: {sorted(self._projected_fields)}. "
                    f"Re-run the query with .select('{name}') or remove the projection."
                )
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    def to_dict(self) -> Dict[str, Any]:
        if self._projected_fields:
            return {f: getattr(self, f, None) for f in self._projected_fields}
        return {f: getattr(self, f, None) for f in self._fields}

    def to_json(self, indent: Optional[int] = None) -> str:
        # Serialize model instance to JSON.
        import json

        return json.dumps(self.to_dict(), indent=indent)

    def to_xml(self) -> str:
        # Serialize model instance to a basic XML string.
        data = self.to_dict()
        lines = [f"<{self.table}>"]
        for k, v in data.items():
            lines.append(f"  <{k}>{v}</{k}>")
        lines.append(f"</{self.table}>")
        return "\n".join(lines)
