from typing import Any, List, Type

import bridge_orm_rs


class RelationDescriptor:
    def __init__(self, target_model: Any):
        self._target_model = target_model

    def _resolve_target(self):
        if isinstance(self._target_model, str):
            from .base import _MODEL_REGISTRY

            # assume it's a table name or class name
            for cls in _MODEL_REGISTRY.values():
                if (
                    cls.__name__ == self._target_model
                    or cls.table == self._target_model
                ):
                    return cls
            raise ImportError(f"Could not resolve model {self._target_model}")
        return self._target_model


class HasMany(RelationDescriptor):
    def __init__(self, target_model: Any, foreign_key: str):
        super().__init__(target_model)
        self.foreign_key = foreign_key

    async def __get__(self, instance, owner):
        if instance is None:
            return self
        target_cls = self._resolve_target()
        raw_results = await bridge_orm_rs.fetch_one_to_many(
            target_cls.table, self.foreign_key, str(instance.id)
        )
        return [target_cls(**res) for res in raw_results]


class BelongsToMany(RelationDescriptor):
    def __init__(self, target_model: Any, junction: str, left_key: str, right_key: str):
        super().__init__(target_model)
        self.junction = junction
        self.left_key = left_key
        self.right_key = right_key

    async def __get__(self, instance, owner):
        if instance is None:
            return self
        target_cls = self._resolve_target()
        raw_results = await bridge_orm_rs.fetch_many_to_many(
            target_cls.table,
            self.junction,
            self.left_key,
            self.right_key,
            str(instance.id),
        )
        return [target_cls(**res) for res in raw_results]


class SelfReferential(RelationDescriptor):
    def __init__(self, target_model: Any, parent_key: str):
        super().__init__(target_model)
        self.parent_key = parent_key

    async def __get__(self, instance, owner):
        if instance is None:
            return self
        target_cls = self._resolve_target()
        raw_results = await bridge_orm_rs.fetch_self_ref(
            target_cls.table, self.parent_key, str(instance.id)
        )
        return [target_cls(**res) for res in raw_results]
