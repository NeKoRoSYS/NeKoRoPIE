from main import BasePayload
# from pydantic import Field
# from typing import Optional

class DiscordUserPayload(BasePayload):
    discord_id: str

class CreateUserPayload(DiscordUserPayload):
    username: str

class UpdateUserPayload(DiscordUserPayload):
    bio: str