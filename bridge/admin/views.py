from typing import Any, Dict, List, Optional, Type
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from ..core.base import BaseModel, _MODEL_REGISTRY
from .auth import get_current_user, require_role, User
from .csrf import csrf_protect, origin_check
from .ratelimit import rate_limit

router = APIRouter(prefix="/admin/api")

MAX_PAGE_SIZE = 100
MAX_OFFSET = 10000
MAX_PAYLOAD_BYTES = 1024 * 1024  # 1 MB


def _reject_oversized_payload(request: Request) -> None:
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large. Maximum size is {MAX_PAYLOAD_BYTES} bytes.",
        )


def sanitize_input(model_cls: Type[BaseModel], data: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = {}
    for field in model_cls._fields:
        if field in data:
            value = data[field]
            if isinstance(value, str) and len(value) > 65535:
                raise HTTPException(
                    status_code=422,
                    detail=f"Field '{field}' exceeds maximum length of 65535 characters",
                )
            sanitized[field] = value
    return sanitized


@router.get("/{table}")
async def list_items(
    table: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=MAX_PAGE_SIZE),
    search: Optional[str] = None,
    user: User = Depends(get_current_user)
):
    model_cls = _MODEL_REGISTRY.get(table)
    if not model_cls:
        raise HTTPException(status_code=404, detail="Table not found")

    offset = (page - 1) * page_size
    if offset > MAX_OFFSET:
        raise HTTPException(
            status_code=422,
            detail=f"Offset exceeds maximum of {MAX_OFFSET}",
        )

    query = model_cls.query()
    if search:
        if model_cls._fields:
            query = query.filter(**{f"{model_cls._fields[0]}__contains": search})

    items = await query.offset(offset).limit(page_size).fetch()
    return {
        "items": [item.to_dict() for item in items],
        "page": page,
        "page_size": page_size
    }


@router.post(
    "/{table}",
    dependencies=[Depends(csrf_protect), Depends(origin_check), Depends(rate_limit)],
)
async def create_item(
    table: str,
    data: Dict[str, Any],
    request: Request,
    user: User = Depends(require_role(["admin"]))
):
    model_cls = _MODEL_REGISTRY.get(table)
    if not model_cls:
        raise HTTPException(status_code=404, detail="Table not found")

    _reject_oversized_payload(request)

    sanitized_data = sanitize_input(model_cls, data)
    item = await model_cls.create(**sanitized_data)
    return item.to_dict()


@router.get("/{table}/{pk}")
async def get_item(
    table: str,
    pk: str,
    user: User = Depends(get_current_user)
):
    model_cls = _MODEL_REGISTRY.get(table)
    if not model_cls:
        raise HTTPException(status_code=404, detail="Table not found")

    item = await model_cls.find_one(id=pk)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item.to_dict()


@router.put(
    "/{table}/{pk}",
    dependencies=[Depends(csrf_protect), Depends(origin_check), Depends(rate_limit)],
)
async def update_item(
    table: str,
    pk: str,
    data: Dict[str, Any],
    request: Request,
    user: User = Depends(require_role(["admin"]))
):
    model_cls = _MODEL_REGISTRY.get(table)
    if not model_cls:
        raise HTTPException(status_code=404, detail="Table not found")

    _reject_oversized_payload(request)

    item = await model_cls.find_one(id=pk)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    sanitized_data = sanitize_input(model_cls, data)
    from ..core.session import begin_session
    async with await begin_session() as session:

        item._session = session
        for k, v in sanitized_data.items():
            setattr(item, k, v)
        await session.flush()

    return {"status": "updated"}


@router.delete(
    "/{table}/{pk}",
    dependencies=[Depends(csrf_protect), Depends(origin_check), Depends(rate_limit)],
)
async def delete_item(
    table: str,
    pk: str,
    user: User = Depends(require_role(["admin"]))
):

    model_cls = _MODEL_REGISTRY.get(table)
    if not model_cls:
        raise HTTPException(status_code=404, detail="Table not found")

    item = await model_cls.find_one(id=pk)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    await item.delete()
    return {"status": "deleted"}
