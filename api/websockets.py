import logic.ws_handler

# MAKE SURE ALL THE FUNCTIONS HERE TAKE THE FOLLOWING PARAMS:
# (websocket, payload, interaction_id)
ROUTES = {
    'create_user': logic.ws_handler.handle_create,
    'read_user': logic.ws_handler.handle_read,
    'update_user': logic.ws_handler.handle_update,
    'delete_user': logic.ws_handler.handle_delete
}