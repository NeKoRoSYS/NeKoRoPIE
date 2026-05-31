from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

class IUserRepository(ABC):
    @abstractmethod
    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def create_user(self, discord_id: str, username: str, bio: str = "Hello World!") -> bool:
        pass
    
    @abstractmethod
    async def get_user(self, discord_id: str) -> dict:
        pass
        
    @abstractmethod
    async def update_user(self, discord_id: str, new_bio: str) -> bool:
        pass
        
    @abstractmethod
    async def delete_user(self, discord_id: str) -> bool:
        pass