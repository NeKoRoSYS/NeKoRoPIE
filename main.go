package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"nekoropie/api/rest"
	"nekoropie/api/websockets"
	"nekoropie/db"
)

type BasePayload struct {
	Action        string `json:"action"`
	InteractionID string `json:"interaction_id"`
}

type HandshakePayload struct {
	BasePayload
	Token string `json:"token"`
}

type DistributedRateLimiter struct {
	rdb        *redis.Client
	maxActions int
	timeframe  float64
	script     *redis.Script
}

func NewDistributedRateLimiter(rdb *redis.Client, maxActions int, timeframe float64) *DistributedRateLimiter {
	luaScript := `
		-- KEYS[1]: the specific rate limit key for this client
		-- ARGV[1]: current timestamp (used as both score and member)
		-- ARGV[2]: the cutoff timestamp (now - timeframe)
		-- ARGV[3]: max allowed actions
		-- ARGV[4]: TTL for the key in seconds

		redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
		local current_count = redis.call('ZCARD', KEYS[1])
		redis.call('EXPIRE', KEYS[1], ARGV[4])
		if tonumber(current_count) < tonumber(ARGV[3]) then
			redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1])
			return 1 -- Allowed
		else
			return 0 -- Blocked
		end
	`
	return &DistributedRateLimiter{
		rdb:        rdb,
		maxActions: maxActions,
		timeframe:  timeframe,
		script:     redis.NewScript(luaScript),
	}
}

func (l *DistributedRateLimiter) IsAllowed(ctx context.Context, clientID string) (bool, error) {
	key := fmt.Sprintf("rate_limit:%s", clientID)
	now := float64(time.Now().UnixNano()) / 1e9
	clearBefore := now - l.timeframe
	ttl := int(l.timeframe) + 2

	result, err := l.script.Run(ctx, l.rdb, []string{key}, now, clearBefore, l.maxActions, ttl).Result()
	if err != nil {
		return false, err
	}

	return result.(int64) == 1, nil
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type WSClient struct {
	Conn *websocket.Conn
	mu   sync.Mutex
}

func (c *WSClient) SendBytes(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.WriteMessage(websocket.TextMessage, data)
}

type Server struct {
	Token       string
	Header      string
	ValkeyURL   string
	JWTSecret   string
	Origins     []string
	InstanceID  string
	ChannelName string

	Router *chi.Mux
	RDB    *redis.Client
	Limit  *DistributedRateLimiter

	localConnections map[string]*WSClient
	connMutex        sync.RWMutex
	wg               sync.WaitGroup
}

func NewServer() *Server {
	_ = godotenv.Load()

	s := &Server{
		Token:            os.Getenv("APITOKEN"),
		Header:           os.Getenv("CLIENTHEADER"),
		ValkeyURL:        os.Getenv("VALKEYURL"),
		JWTSecret:        os.Getenv("JWTSECRET"),
		InstanceID:       uuid.New().String(),
		localConnections: make(map[string]*WSClient),
	}

	s.ChannelName = fmt.Sprintf("ws_instance:%s", s.InstanceID)

	rawOrigins := os.Getenv("ORIGINS")
	if rawOrigins != "" {
		s.Origins = strings.Split(rawOrigins, ",")
	} else {
		s.Origins = []string{}
	}

	if s.Token == "" || s.Header == "" || s.ValkeyURL == "" || s.JWTSecret == "" || len(s.Origins) == 0 {
		log.Fatal("FATAL ERROR: Environment variables are not set or empty in .env file.")
	}

	s.Router = chi.NewRouter()
	s.Router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   s.Origins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Client-ID", "Authorization", "Content-Type"},
		AllowCredentials: true,
	}))

	rest.RegisterRoutes(s.Router)
	s.Router.HandleFunc("/ws", s.WebSocketEndpoint)

	return s
}

func (s *Server) Start(ctx context.Context) {
	log.Println("Initializing database indexes...")
	db.InitializeAll()

	log.Println("Connecting to Valkey...")
	opts, err := redis.ParseURL(s.ValkeyURL)
	if err != nil {
		log.Fatalf("Invalid Valkey URL: %v", err)
	}
	s.RDB = redis.NewClient(opts)

	s.Limit = NewDistributedRateLimiter(s.RDB, 25, 1.0)

	s.wg.Add(2)
	go s.dlqArchiver(ctx)
	go s.valkeyPubSubListener(ctx)
}

func (s *Server) dlqArchiver(ctx context.Context) {
	defer s.wg.Done()
	time.Sleep(10 * time.Second)

	ticker := time.NewTicker(300 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			iter := s.RDB.Scan(ctx, 0, "dlq:*", 0).Iterator()
			for iter.Next(ctx) {
				key := iter.Val()
				var messages [][]byte

				for {
					msg, err := s.RDB.LPop(ctx, key).Bytes()
					if err == redis.Nil {
						break
					} else if err != nil {
						log.Printf("DLQ LPop error: %v", err)
						break
					}
					messages = append(messages, msg)
				}

				if len(messages) > 0 {
					go s.writeDLQLogs(key, messages)
					log.Printf("Archived %d dropped messages from %s", len(messages), key)
				}
			}

			if err := iter.Err(); err != nil {
				log.Printf("DLQ scan error: %v", err)
			}
		}
	}
}

func (s *Server) writeDLQLogs(key string, messages [][]byte) {
	f, err := os.OpenFile("dead_letters.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Failed to open DLQ log: %v", err)
		return
	}
	defer f.Close()

	for _, msg := range messages {
		entry := map[string]interface{}{
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
			"target_key": key,
			"payload":    json.RawMessage(msg),
		}
		b, _ := json.Marshal(entry)
		f.Write(append(b, '\n'))
	}
}

func (s *Server) valkeyPubSubListener(ctx context.Context) {
	defer s.wg.Done()
	pubsub := s.RDB.Subscribe(ctx, s.ChannelName)
	defer pubsub.Close()
	log.Printf("Instance %s subscribed to routing bus channel: %s", s.InstanceID, s.ChannelName)
	ch := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			log.Println("Valkey Pub/Sub listener routine shutdown.")
			return
		case msg := <-ch:
			var packet map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &packet); err != nil {
				log.Printf("Failed to unmarshal pubsub msg: %v", err)
				continue
			}

			targetID, ok1 := packet["target_client_id"].(string)
			payloadData, ok2 := packet["data"]
			if !ok1 || !ok2 {
				continue
			}

			s.connMutex.RLock()
			client, exists := s.localConnections[targetID]
			s.connMutex.RUnlock()

			if exists {
				bytesData, _ := json.Marshal(payloadData)
				client.SendBytes(bytesData)
			} else {
				log.Printf("Client %s offline. Pushing to DLQ.", targetID)
				dlqKey := fmt.Sprintf("dlq:%s", targetID)
				payloadBytes, _ := json.Marshal(payloadData)
				
				pipe := s.RDB.Pipeline()
				pipe.RPush(ctx, dlqKey, payloadBytes)
				pipe.LTrim(ctx, dlqKey, -100, -1)
				pipe.Exec(ctx)

				_, err := pipe.Exec(ctx)
				if err != nil {
                    log.Printf("DLQ push failed: %v", err)
                }
			}
		}
	}
}

func (s *Server) RouteMessage(ctx context.Context, targetClientID string, payloadData interface{}) bool {
        s.connMutex.RLock()
        client, exists := s.localConnections[targetClientID]
        s.connMutex.RUnlock()
        payloadBytes, _ := json.Marshal(payloadData)
        if exists {
                if err := client.SendBytes(payloadBytes); err != nil {
                        log.Printf("Failed to send local message to %s: %v", targetClientID, err)
                        return false
                }
                return true
        }
        targetInstanceID, err := s.RDB.Get(ctx, fmt.Sprintf("client_route:%s", targetClientID)).Result()
        if err == nil && targetInstanceID != "" {
                packet := map[string]interface{}{
                        "target_client_id": targetClientID,
                        "data":             payloadData,
                }
                packetBytes, _ := json.Marshal(packet)
                s.RDB.Publish(ctx, fmt.Sprintf("ws_instance:%s", targetInstanceID), packetBytes)
                return true
        }
        log.Printf("Could not route message: Client %s is offline or not mapped.", targetClientID)
        return false
}

func (s *Server) HandleHandshake(conn *websocket.Conn, payload []byte, interactionID string) bool {
	var data HandshakePayload
	if err := json.Unmarshal(payload, &data); err != nil {
		s.sendError(conn, fmt.Sprintf("Invalid payload format: %v", err), interactionID)
		return false
	}
	return true
}

func (s *Server) sendError(conn *websocket.Conn, msg string, interactionID string) {
	b, _ := json.Marshal(map[string]interface{}{
		"error":          true,
		"message":        msg,
		"interaction_id": interactionID,
	})
	conn.WriteMessage(websocket.TextMessage, b)
}

func (s *Server) WebSocketEndpoint(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	expectedAuth := "Bearer " + s.Token

	if subtle.ConstantTimeCompare([]byte(authHeader), []byte(expectedAuth)) != 1 {
		log.Println("Blocked connection: Invalid Authorization header.")
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade websocket: %v", err)
		return
	}
	defer conn.Close()

	conn.SetReadLimit(1048576)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, firstMessage, err := conn.ReadMessage()
	if err != nil {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "Handshake Error"))
		return
	}
	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		return nil
	})

	var baseData BasePayload
	if err := json.Unmarshal(firstMessage, &baseData); err != nil || baseData.Action != "handshake" {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "Handshake Required"))
		return
	}

	var handshakeData HandshakePayload
	if err := json.Unmarshal(firstMessage, &handshakeData); err != nil || handshakeData.Token == "" {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "JWT Required"))
		return
	}

	token, err := jwt.Parse(handshakeData.Token, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok || token.Method.Alg() != "HS256" {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return []byte(s.JWTSecret), nil
	})

	if err != nil || !token.Valid {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "Invalid Token"))
		return
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "Invalid Token Claims"))
		return
	}
	clientID, ok := claims["sub"].(string)
	if !ok {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "JWT missing 'sub' claim"))
		return
	}

	if !s.HandleHandshake(conn, firstMessage, baseData.InteractionID) {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1008, "Database Handshake Rejected"))
		return
	}

	wsClient := &WSClient{Conn: conn}
	s.connMutex.Lock()
	s.localConnections[clientID] = wsClient
	s.connMutex.Unlock()

	ctx := context.Background()
	s.RDB.Set(ctx, fmt.Sprintf("client_route:%s", clientID), s.InstanceID, 0)
	log.Printf("Client %s authenticated and routed to %s.", clientID, s.InstanceID)

	dlqKey := fmt.Sprintf("dlq:%s", clientID)
	queuedMsgs, _ := s.RDB.LRange(ctx, dlqKey, 0, -1).Result()
	if len(queuedMsgs) > 0 {
		for _, msg := range queuedMsgs {
			wsClient.SendBytes([]byte(msg))
		}
		s.RDB.Del(ctx, dlqKey)
	}

	pingerDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				wsClient.mu.Lock()
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					wsClient.mu.Unlock()
					return
				}
				wsClient.mu.Unlock()
			case <-pingerDone:
				return
			}
		}
	}()

	defer func() {
		close(pingerDone)
		s.connMutex.Lock()
		delete(s.localConnections, clientID)
		s.connMutex.Unlock()

		script := `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`
		s.RDB.Eval(ctx, script, []string{fmt.Sprintf("client_route:%s", clientID)}, s.InstanceID)
		s.RDB.Del(ctx, fmt.Sprintf("rate_limit:%s", clientID))
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		if allowed, _ := s.Limit.IsAllowed(ctx, clientID); !allowed {
			s.sendError(conn, "Too many requests.", "")
			continue
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(message, &payload); err != nil {
			continue
		}

		actionVal, okAction := payload["action"].(string)
		if !okAction {
			log.Printf("Dropped payload: missing action")
			continue
		}

		interactionID, _ := payload["interaction_id"].(string)

		handler, exists := websockets.ROUTES[actionVal]
		if exists {
			err := handler(wsClient.SendBytes, payload, interactionID)
			if err != nil {
				log.Printf("Handler error for %s: %v", actionVal, err)
				s.sendError(conn, "Internal server error.", interactionID)
			}
		} else {
			log.Printf("Unknown action: %s", actionVal)
		}
	}
}

func main() {
	server := NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.Start(ctx)

	httpServer := &http.Server{
		Addr:    ":8000",
		Handler: server.Router,
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Server listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-c
	log.Println("\nGracefully shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	
	httpServer.Shutdown(shutdownCtx)
	cancel()
	server.wg.Wait()
	server.RDB.Close()
}