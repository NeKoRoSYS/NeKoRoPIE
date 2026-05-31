from db.db_factory import db

async def create_user(discord_id: str, username: str) -> bool:
    return await db.user_repo.create_user(discord_id, username)

async def get_user(discord_id: str) -> dict | None:
    return await db.user_repo.get_user(discord_id)

async def update_user(discord_id: str, bio: str) -> bool:
    return await db.user_repo.update_user(discord_id, bio)

async def delete_user(discord_id: str) -> bool:
    return await db.user_repo.delete_user(discord_id)