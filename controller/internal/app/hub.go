package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]map[string]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: map[*websocket.Conn]map[string]struct{}{}}
}

func (h *Hub) Add(c *websocket.Conn, topics []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := map[string]struct{}{}
	for _, t := range topics {
		if t != "" {
			set[t] = struct{}{}
		}
	}
	h.clients[c] = set
}

func (h *Hub) Remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

func (h *Hub) Broadcast(topic string, msg any) {
	raw, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c, topics := range h.clients {
		if _, ok := topics[topic]; !ok && len(topics) > 0 {
			continue
		}
		_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err := c.WriteMessage(websocket.TextMessage, raw); err != nil {
			_ = c.Close()
			delete(h.clients, c)
		}
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

func boolPtr(v bool) *bool { return &v }

func int32Ptr(v int32) *int32 { return &v }

func slogInfo(msg string, args ...any) { slog.Info(msg, args...) }

func slogErr(msg string, err error) {
	if err != nil {
		slog.Error(msg, "err", err)
	}
}

func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func must(err error, what string) {
	if err != nil {
		panic(fmt.Errorf("%s: %w", what, err))
	}
}
