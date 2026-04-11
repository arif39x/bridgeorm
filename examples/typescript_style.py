import asyncio

from bridge_orm import Post, User, connect
from bridge_orm.exceptions import NotFoundError


async def main():
    # QLite for easy local testing
    await connect("sqlite://:memory:")

    # For this prototype, assume the tables 'users' and 'posts' exist Not Migrration

    print("Creating user Miku..")
    alice = await User.create(username="alice", email="bluehairbluetie@gmail.com")
    print(f"Created Miku with ID: {Miku.id}")

    print("\nQuerying fore Miku by email...")
    results = (
        await User.query().filter(email="bluehairbluetie@gmail.com").limit(10).fetch()
    )
    for user in results:
        print(f"Found: {user.username} ({user.email})")

    print("\nLoading related posts (none expected)...")
    posts = await alice.load_related(Post)
    print(f"Posts found: {len(posts)}")

    print("\nAttempting to find nonexistent user...")
    try:
        await User.find_one(id="00000000-0000-0000-0000-000000000000")
    except NotFoundError as e:
        print(f"Caught expected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
