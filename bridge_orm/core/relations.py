from typing import Type, Any
import bridge_orm_rs

class RelationDescriptor:
    def __init__(self, target_model: Any):
        self.target_model = target_model

class HasMany(RelationDescriptor):
    def __init__(self, target_model: Any, foreign_key: str):
        super().__init__(target_model)
        self.foreign_key = foreign_key

    def __get__(self, instance, owner):
        if instance is None: return self
        # Triggers the Rust fetch
        return bridge_orm_rs.load_related_posts(str(instance.id))

class BelongsToMany(RelationDescriptor):
    def __init__(self, target_model: Any, junction: str, left_key: str, right_key: str):
        super().__init__(target_model)
        self.junction = junction
        self.left_key = left_key
        self.right_key = right_key

    def __get__(self, instance, owner):
        if instance is None: return self
        # Conceptual implementation for prototype
        return []

class SelfReferential(RelationDescriptor):
    def __init__(self, target_model: Any, parent_key: str):
        super().__init__(target_model)
        self.parent_key = parent_key

    def __get__(self, instance, owner):
        if instance is None: return self
        return None
