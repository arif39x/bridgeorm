import asyncio
from bridge_orm import connect, User, Post
from bridge_orm.relations import HasMany, BelongsToMany, SelfReferential

# Define advanced relations on existing models
User.posts = HasMany(Post, foreign_key="user_id")
User.followers = BelongsToMany(
    User, junction="user_followers",
    left_key="follower_id", right_key="followee_id"
)
User.manager = SelfReferential(User, parent_key="manager_id")

async def main():
    await connect("sqlite://:memory:")

    print("Defining relations on User model...")
    print(f"User.posts: {type(User.posts)}")
    print(f"User.followers: {type(User.followers)}")
    print(f"User.manager: {type(User.manager)}")

    alice = await User.create(username="alice", email="alice@example.com")
    
    # Accessing relations triggers the descriptors
    print("\nAccessing alice.posts (calls Rust load_related_posts)...")
    posts = await alice.posts
    print(f"Found {len(posts)} posts for Alice")

    print("\nAccessing alice.followers (conceptual many-to-many)...")
    followers = await alice.followers
    print(f"Followers: {followers}")

if __name__ == "__main__":
    asyncio.run(main())
