package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // Allow all for dev
}

// SignalMessage is the generic envelope for all WebRTC signaling
type SignalMessage struct {
	Type    string          `json:"type"` // "join", "offer", "answer", "ice"
	Room    string          `json:"room"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Client represents a single WebSocket connection
type Client struct {
	conn *websocket.Conn
	room string
	send chan []byte
}

// Room manages a group of clients
type Room struct {
	clients map[*Client]bool
	mu      sync.RWMutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.RWMutex
)

func getOrCreateRoom(id string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	if r, ok := rooms[id]; ok {
		return r
	}
	r := &Room{clients: make(map[*Client]bool)}
	rooms[id] = r
	return r
}

func (r *Room) add(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c] = true
}

func (r *Room) remove(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, c)
}

// broadcastExcept sends to all peers in the room except the sender
func (r *Room) broadcastExcept(sender *Client, msg []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for client := range r.clients {
		if client != sender {
			select {
			case client.send <- msg:
			default:
				// Skip slow clients to avoid blocking
			}
		}
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	client := &Client{conn: conn, send: make(chan []byte, 256)}

	// Writer goroutine (Gorilla requires single writer)
	go func() {
		for msg := range client.send {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	// Cleanup on disconnect
	defer func() {
		if client.room != "" {
			room := getOrCreateRoom(client.room)
			room.remove(client)
		}
		close(client.send)
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var msg SignalMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "join":
			client.room = msg.Room
			room := getOrCreateRoom(msg.Room)
			room.add(client)
			log.Printf("Client joined room: %s", msg.Room)
		case "offer", "answer", "ice":
			// Forward to everyone else in the room
			if client.room != "" {
				room := getOrCreateRoom(client.room)
				room.broadcastExcept(client, raw)
			}
		}
	}
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	// Suga injects PORT usually, but hardcode 8080 for local testing
	log.Println("Signaling server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
