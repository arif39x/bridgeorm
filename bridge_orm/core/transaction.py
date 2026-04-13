from contextlib import asynccontextmanager
import bridge_orm_rs

@asynccontextmanager
async def transaction():
    """Async context manager for atomic database transactions."""
    tx = await bridge_orm_rs.begin_transaction()
    try:
        yield tx
        await tx.commit()
    except Exception as e:
        await tx.rollback()
        raise e
