from contextlib import asynccontextmanager
import bridge_orm_rs

@asynccontextmanager
async def transaction():
    """Async context manager for atomic database transactions."""
    tx = bridge_orm_rs.begin_transaction()
    try:
        yield tx
        # For this prototype, commit/rollback are stubs in the Rust side 
        # as we are focusing on the architecture and API surface.
        tx.commit()
    except Exception as e:
        tx.rollback()
        raise e
