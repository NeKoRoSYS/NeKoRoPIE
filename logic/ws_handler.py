# import os
from db.db_schemas import CreateUserPayload, DiscordUserPayload, UpdateUserPayload
from db.db_controller import create_user, get_user, update_user, delete_user
from pydantic import ValidationError
# from dotenv import load_dotenv
from pymongo.errors import DuplicateKeyError

async def handle_error(websocket, message: str, interaction_id: str = None):
    await websocket.send_json({
        "interaction_id": interaction_id, 
        "error": True, 
        "message": message
    })

# load_dotenv()
# TOKEN = os.getenv('APITOKEN')

# if not TOKEN:
#    raise ValueError("FATAL ERROR: Environment variable 'TOKEN' is not set in .env file.")

async def handle_create(websocket, payload, interaction_id):
    try:
        data = CreateUserPayload(**payload)
    except ValidationError as e:
        await websocket.send_json({
            "interaction_id": interaction_id, 
            "error": True, 
            "message": f"Invalid payload format: {e.errors()[0]['msg']}"
        })
        return
    discord_id = data.discord_id
    username = data.username
    
    vk = websocket.app.state.ws_server.vk
    lock_key = f"link_lock:{discord_id}"
    acquired_lock = await vk.set(lock_key, "locked", nx=True, ex=5)
    
    if not acquired_lock:
        await websocket.send_json({"error": True, "interaction_id": interaction_id, "message": "A link request is already processing. Please wait."})
        return
    
    try:
        success = await create_user(discord_id, username)
        if success:
            await websocket.send_json({"event": "created", "interaction_id": interaction_id})
        else:
            await websocket.send_json({"error": True, "interaction_id": interaction_id, "message": "User already exists!"})
        pass
    finally:
        await vk.delete(lock_key)
        
async def handle_read(websocket, payload, interaction_id):
    try:
        data = DiscordUserPayload(**payload)
    except ValidationError as e:
        await websocket.send_json({
            "interaction_id": interaction_id, 
            "error": True, 
            "message": f"Invalid payload format: {e.errors()[0]['msg']}"
        })
        return
    user = await get_user(data.discord_id)
    if user:
        await websocket.send_json({"event": "read", "interaction_id": interaction_id, "data": user})
    else:
        await websocket.send_json({"error": True, "interaction_id": interaction_id, "message": "User not found."})

async def handle_update(websocket, payload, interaction_id):
    try:
        data = UpdateUserPayload(**payload)
    except ValidationError as e:
        await websocket.send_json({
            "interaction_id": interaction_id, 
            "error": True, 
            "message": f"Invalid payload format: {e.errors()[0]['msg']}"
        })
        return
    success = await update_user(data.discord_id, data.bio)
    if success:
        await websocket.send_json({"event": "updated", "interaction_id": interaction_id})
    else:
        await websocket.send_json({"error": True, "interaction_id": interaction_id, "message": "User not found or bio unchanged."})

async def handle_delete(websocket, payload, interaction_id):
    try:
        data = DiscordUserPayload(**payload)
    except ValidationError as e:
        await websocket.send_json({
            "interaction_id": interaction_id, 
            "error": True, 
            "message": f"Invalid payload format: {e.errors()[0]['msg']}"
        })
        return
    success = await delete_user(data.discord_id)
    if success:
        await websocket.send_json({"event": "deleted", "interaction_id": interaction_id})
    else:
        await websocket.send_json({"error": True, "interaction_id": interaction_id, "message": "User not found."})
