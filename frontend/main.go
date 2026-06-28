// Command frontend is the edge of the system. It serves the single-page UI
// (which also hosts the browser-based client tier) and exposes the two RPCs
// from the design:
//
//	StartWork   -> enqueue a work item, return an operation id
//	GetOperation -> report whether that operation has completed
//
// It learns about completions by subscribing to the Redis pub/sub channel the
// backend publishes to (the "message queue" in the design).
//
// Phase 0: no load shedding yet — every request is accepted. That arrives in a
// later phase.
package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed static/*
var staticFS embed.FS

const (
	workQueueKey   = "work:queue"
	completionChan = "completions"
)

type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
}

type server struct {
	rdb  *redis.Client
	done sync.Map // operation_id -> struct{}, populated from pub/sub
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	s := &server{rdb: rdb}

	// Listen for completion notifications from the backend.
	go s.subscribeCompletions(context.Background())

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("embed static: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))
	mux.HandleFunc("/api/start", s.handleStart)
	mux.HandleFunc("/api/op", s.handleGetOp)
	mux.HandleFunc("/api/metrics", s.handleMetrics)

	listen := ":" + getenv("PORT", "8080")
	log.Printf("frontend listening on %s (redis=%s)", listen, addr)
	log.Fatal(http.ListenAndServe(listen, mux))
}

func (s *server) subscribeCompletions(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, completionChan)
	for msg := range sub.Channel() {
		s.done.Store(msg.Payload, struct{}{})
	}
}

// handleStart implements the StartWork RPC.
func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	id := randHex()
	item := workItem{ID: id, EnqueuedMs: time.Now().UnixMilli()}
	b, _ := json.Marshal(item)
	if err := s.rdb.LPush(r.Context(), workQueueKey, b).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"operation_id": id})
}

// handleGetOp implements the GetOperation RPC.
func (s *server) handleGetOp(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	status := "pending"
	if _, ok := s.done.Load(id); ok {
		status = "done"
	} else if v, _ := s.rdb.Get(r.Context(), "op:"+id).Result(); v == "done" {
		// Fallback: the pub/sub notification may have been missed (e.g. the
		// frontend restarted). The Redis key is the durable source of truth.
		status = "done"
	}
	writeJSON(w, map[string]string{"status": status})
}

// handleMetrics exposes server-side counters the browser can't see for itself.
// Phase 0: just the work-queue depth. The full metric stream lands in Phase 2.
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	qlen, _ := s.rdb.LLen(r.Context(), workQueueKey).Result()
	writeJSON(w, map[string]any{"queue_len": qlen})
}

func randHex() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
