package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type SignalMessage struct {
	Type    string          `json:"type"`
	Room    string          `json:"room"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type Client struct {
	conn *websocket.Conn
	room string
	send chan []byte
}

type Room struct {
	clients map[*Client]bool
	mu      sync.RWMutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.RWMutex

	startTime     = time.Now()
	totalConns    int64
	totalMessages int64
	statsMu       sync.Mutex
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

func (r *Room) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

func (r *Room) broadcastExcept(sender *Client, msg []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for client := range r.clients {
		if client != sender {
			select {
			case client.send <- msg:
			default:
			}
		}
	}
}

func cleanupEmptyRooms() {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	for id, r := range rooms {
		r.mu.RLock()
		empty := len(r.clients) == 0
		r.mu.RUnlock()
		if empty {
			delete(rooms, id)
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

	statsMu.Lock()
	totalConns++
	statsMu.Unlock()

	client := &Client{conn: conn, send: make(chan []byte, 256)}

	go func() {
		for msg := range client.send {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	defer func() {
		if client.room != "" {
			room := getOrCreateRoom(client.room)
			room.remove(client)
		}
		close(client.send)
		cleanupEmptyRooms()
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}

		statsMu.Lock()
		totalMessages++
		statsMu.Unlock()

		var msg SignalMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "join":
			client.room = msg.Room
			room := getOrCreateRoom(msg.Room)

			// Snapshot existing count BEFORE adding this client,
			// so the first arrival is deterministically host.
			existing := room.count()

			room.add(client)

			role := "guest"
			if existing == 0 {
				role = "host"
			}

			roleMsg, _ := json.Marshal(map[string]string{
				"type": "role",
				"role": role,
				"room": msg.Room,
			})
			select {
			case client.send <- roleMsg:
			default:
			}

			log.Printf("Client joined room %q as %s (%d existing)", msg.Room, role, existing)

		case "offer", "answer", "ice":
			if client.room != "" {
				room := getOrCreateRoom(client.room)
				room.broadcastExcept(client, raw)
			}
		}
	}
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	roomsMu.RLock()
	roomCount := len(rooms)

	type RoomInfo struct {
		ID      string `json:"id"`
		Clients int    `json:"clients"`
	}
	roomList := make([]RoomInfo, 0, len(rooms))
	liveClients := 0
	for id, rm := range rooms {
		n := rm.count()
		liveClients += n
		roomList = append(roomList, RoomInfo{ID: id, Clients: n})
	}
	roomsMu.RUnlock()

	statsMu.Lock()
	conns := totalConns
	msgs := totalMessages
	statsMu.Unlock()

	resp := map[string]interface{}{
		"uptime_seconds": int(time.Since(startTime).Seconds()),
		"uptime_human":   time.Since(startTime).Round(time.Second).String(),
		"total_conns":    conns,
		"total_messages": msgs,
		"live_clients":   liveClients,
		"room_count":     roomCount,
		"rooms":          roomList,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(resp)
}

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Signaling Server Stats</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #0d1117; color: #c9d1d9; }
  h1 { font-size: 20px; margin: 0 0 16px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 24px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px; }
  .card .label { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
  .card .value { font-size: 26px; font-weight: 600; margin-top: 4px; color: #58a6ff; }
  table { width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #21262d; font-size: 14px; }
  th { color: #8b949e; font-weight: 500; font-size: 12px; text-transform: uppercase; }
  tr:last-child td { border-bottom: none; }
  .status { font-size: 12px; color: #3fb950; margin-top: 12px; }
  .status.offline { color: #f85149; }
  code { background: #21262d; padding: 2px 6px; border-radius: 4px; font-size: 12px; }
</style>
</head>
<body>
<h1>Signaling Server — Live Stats</h1>
<div class="grid">
  <div class="card"><div class="label">Uptime</div><div class="value" id="uptime">–</div></div>
  <div class="card"><div class="label">Live Clients</div><div class="value" id="live">–</div></div>
  <div class="card"><div class="label">Rooms</div><div class="value" id="rooms">–</div></div>
  <div class="card"><div class="label">Total Conns</div><div class="value" id="conns">–</div></div>
  <div class="card"><div class="label">Total Messages</div><div class="value" id="msgs">–</div></div>
</div>
<h3 style="font-size:14px;color:#8b949e;text-transform:uppercase;margin:0 0 8px;">Active Rooms</h3>
<table>
  <thead><tr><th>Room ID</th><th>Clients</th></tr></thead>
  <tbody id="roomTable"><tr><td colspan="2" style="color:#8b949e;">No active rooms</td></tr></tbody>
</table>
<div class="status" id="status">● Connecting…</div>
<p style="font-size:12px;color:#8b949e;margin-top:20px;">
  WebSocket endpoint: <code id="wsurl"></code> · Refreshes every 2s
</p>
<script>
const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
document.getElementById('wsurl').textContent = wsProto + '://' + location.host + '/ws';
async function refresh() {
  const statusEl = document.getElementById('status');
  try {
    const res = await fetch('/stats');
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const d = await res.json();
    document.getElementById('uptime').textContent = d.uptime_human;
    document.getElementById('live').textContent = d.live_clients;
    document.getElementById('rooms').textContent = d.room_count;
    document.getElementById('conns').textContent = d.total_conns;
    document.getElementById('msgs').textContent = d.total_messages;
    const tbody = document.getElementById('roomTable');
    if (d.rooms.length === 0) {
      tbody.innerHTML = '<tr><td colspan="2" style="color:#8b949e;">No active rooms</td></tr>';
    } else {
      tbody.innerHTML = d.rooms.map(r => '<tr><td>' + r.id + '</td><td>' + r.clients + '</td></tr>').join('');
    }
    statusEl.textContent = '● Live';
    statusEl.className = 'status';
  } catch (e) {
    statusEl.textContent = '● Offline — ' + e.message;
    statusEl.className = 'status offline';
  }
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>`

func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

func main() {
	port := os.Getenv("SUGA_PUBLIC_TARGET_PORT")
	if port == "" {
		port = os.Getenv("PORT")
	}
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/stats", statsHandler)
	http.HandleFunc("/", dashboardHandler)

	log.Println("Signaling server on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
