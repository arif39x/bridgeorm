import asyncio

import bridge_orm
from bridge_orm import Post, User, transaction
from bridge_orm.exceptions import HookAbortedError

bridge_orm.configure_logging(level="debug", slow_query_ms=50)


async def main():
    # Use SQLite for testing
    await bridge_orm.connect("sqlite://:memory:")

    # Lifecycle hook cancel invalid creates
    @User.before_create
    async def require_email(u):
        if not u.email:
            print("Hook: Email is missing, aborting...")
            return False
        return True

    print("Testing Hook cancellation...")
    try:
        await User.create(username="ghost", email="")
    except HookAbortedError:
        print("Caught expected HookAbortedError")

    # Transaction + rollback
    print("\nTesting Transaction context...")
    try:
        async with transaction() as tx:
            miku = await User.create(username="miku", email="a@example.com", tx=tx)
            print(f"Created {miku.username} in transaction")
            raise Exception("Simulated failure")
    except Exception as e:
        print(f"Transaction rolled back due to: {e}")

    # Eager loading chain
    print("\nTesting Eager loading API...")
    users = await User.query().eager(posts=Post).fetch()
    print("Eager query executed successfully")

    # Introspection
    print("\nTesting Introspection...")
    from bridge_orm.introspect import reflect_table

    try:
        model_src = await reflect_table("users")
        print("Generated Model Source:")
        print(model_src)
    except Exception as e:
        print(f"Introspection failed (expected if information_schema missing): {e}")


if __name__ == "__main__":
    asyncio.run(main())
