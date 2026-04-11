from typing import Any, Dict, List, Optional, Type

import bridge_orm_rs

from .exceptions import HookAbortedError, NotFoundError
from .hooks import dispatch_hooks
from .query import QueryBuilder


class BaseModel:
    table: str = ""
    _fields: List[str] = []

    @classmethod
    def _get_hooks(cls):
        if "_hooks" not in cls.__dict__:
            cls._hooks = {}
        return cls._hooks

    @classmethod
    def before_create(cls, func):
        hooks = cls._get_hooks()
        if "before_create" not in hooks:
            hooks["before_create"] = []
        hooks["before_create"].append(func)
        return func

    @classmethod
    def after_create(cls, func):
        hooks = cls._get_hooks()
        if "after_create" not in hooks:
            hooks["after_create"] = []
        hooks["after_create"].append(func)
        return func

    @classmethod
    def query(cls) -> QueryBuilder:
        return QueryBuilder(cls)

    @classmethod
    async def create(cls, tx=None, **kwargs) -> Any:
        # Create instance locally for hooks
        class TempInstance:
            pass

        instance = TempInstance()
        for k, v in kwargs.items():
            setattr(instance, k, v)

        await dispatch_hooks(cls, "before_create", instance)

        if cls.table == "users":
            res = bridge_orm_rs.create_user(kwargs["username"], kwargs["email"])
        elif cls.table == "posts":
            res = bridge_orm_rs.create_post(kwargs["title"], str(kwargs["user_id"]))
        else:
            raise NotImplementedError

        await dispatch_hooks(cls, "after_create", res)
        return res

    @classmethod
    async def find_one(cls, tx=None, **kwargs) -> Optional[Any]:
        id = kwargs.get("id")
        if cls.table == "users":
            res = bridge_orm_rs.find_user_by_id(str(id))
            if res is None:
                raise NotFoundError(f"No {cls.__name__} found with id={id}")
            return res
        return None
