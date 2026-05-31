package websockets

import (
	"encoding/json"
	"fmt"
)

type SendFunc func(data []byte) error
type HandlerFunc func(send SendFunc, payload map[string]interface{}, interactionID string) error

var ROUTES = map[string]HandlerFunc{
	"ping": handlePing,
}

func handlePing(send SendFunc, payload map[string]interface{}, interactionID string) error {
	response := map[string]interface{}{
		"event":          "pong",
		"interaction_id": interactionID,
	}
	bytes, _ := json.Marshal(response)
	return send(bytes)
}