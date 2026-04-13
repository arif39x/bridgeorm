import pytest
import pytest_asyncio
import asyncio
import os
from bridge_orm import connect, User, execute_raw

@pytest_asyncio.fixture
async def db_setup(request):
    db_file = f"identity_test_{request.node.name}.db"
    if os.path.exists(db_file):
        os.remove(db_file)
    await connect(f"sqlite:{db_file}?mode=rwc")
    await execute_raw("""
        CREATE TABLE users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    yield
    if os.path.exists(db_file):
        os.remove(db_file)

@pytest.mark.asyncio
async def test_identity_map_same_task(db_setup):
    """Verify that fetching the same ID in the same task returns the same object instance."""
    miku = await User.create(username="Miku", email="miku@vocaloid.com")
    
    # First fetch
    user_a = await User.find_one(id=miku.id)
    # Second fetch
    user_b = await User.find_one(id=miku.id)
    
    # Assert physical identity (referential integrity)
    assert user_a is user_b
    assert id(user_a) == id(user_b)
    
    # Mutation on one affects the other (in-memory)
    user_a.username = "Hatsune Miku"
    assert user_b.username == "Hatsune Miku"

@pytest.mark.asyncio
async def test_identity_map_different_tasks(db_setup):
    """Verify that different asyncio tasks have isolated identity maps."""
    miku = await User.create(username="Miku", email="miku@vocaloid.com")
    
    async def get_user_with_sleep():
        # Force task switching
        await asyncio.sleep(0.01)
        u = await User.find_one(id=miku.id)
        # Manually verify cache content if possible
        return u
    
    # Run two tasks
    task_a = asyncio.create_task(get_user_with_sleep())
    task_b = asyncio.create_task(get_user_with_sleep())
    
    user_a = await task_a
    user_b = await task_b
    
    # If this fails, it means the test runner might be using a single context 
    # for all tasks or there's a leak in the ORM.
    assert user_a.id == user_b.id
    # We'll use id() check for clarity
    assert id(user_a) != id(user_b)
