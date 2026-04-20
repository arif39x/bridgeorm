import asyncio
from bridge_orm import User, Post, connect, transaction, execute_raw

async def main():
    # Connect to a real file
    print("Connecting to test.db...")
    await connect("sqlite:test.db?mode=rwc")

    # Setup Schema safely
    print("Setting up schema...")
    await execute_raw("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    await execute_raw("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            user_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)

    # Create data
    print("Creating User...")
    teto = await User.create(username="Teto", email="kasaneteto@gmail.com")
    print(f"Created: {teto.username} (ID: {teto.id})")

    # Create post
    print("Creating Post...")
    await Post.create(title="Hello BridgeORM!", user_id=teto.id)

if __name__ == "__main__":
    asyncio.run(main())
