import asyncio

from bridge_orm import Post, User, connect


async def main():
    # Use SQLite
    await connect("sqlite://:memory:")

    # Create SQLAlchemy style
    alice = await User.create(username="Miku", email="bluehairbluetie@gmail.com")
    print(f"Created: {alice.username}")

    # Fetch with filter Django style
    found = await User.query().filter(username="Miku").fetch()
    for user in found:
        print(f"Found user: {user.username}")

    # Load related concise
    posts = await alice.load_related(Post)
    print(f"Number of posts: {len(posts)}")


if __name__ == "__main__":
    asyncio.run(main())
