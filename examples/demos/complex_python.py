import asyncio
import uuid
from datetime import datetime, timezone
from bridge_orm import BaseModel, begin_session, Raw
import bridge_orm_rs

class User(BaseModel):
    table = "users"
    _fields = ["id", "username", "metadata"]
    _primary_keys = ["id"]
    id: uuid.UUID
    username: str
    metadata: dict

async def run_demo():
    # In a real scenario, use actual Postgres URL
    await bridge_orm_rs.connect("postgresql://postgres:postgres@localhost:5432/bridgeorm")
    
    async with await begin_session() as session:
        # Complex data: UUID, JSON dict, and Raw SQL for the username
        user_id = uuid.uuid4()
        user = await User.create(
            session,
            id=user_id,
            username=Raw("UPPER({})", "complex_user_python"),
            metadata={
                "role": "admin",
                "tags": ["performance", "rust", "python"],
                "last_active": datetime.now(timezone.utc).isoformat()
            }
        )
        print(f"Python: Stored complex User {user.username} (ID: {user.id})")
        print(f"Metadata stored as JSON: {user.metadata}")

if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except Exception as e:
        print(f"Execution failed (Check if Postgres is running): {e}")
