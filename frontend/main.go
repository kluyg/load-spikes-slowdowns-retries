// Command frontend is the edge of the system. It serves the single-page UI
// (which also hosts the browser-based client tier) and exposes:
//
//	POST /api/start          StartWork    — enqueue a work item, return an op id
//	GET  /api/op?id=         GetOperation — report whether that op has completed
//	GET  /api/stream         SSE stream of server-side metrics
//	POST /api/chaos/latency  trigger a short 4x backend-latency spike
//
// Completions are learned by subscribing to the Redis pub/sub channel the
// backend publishes to (the "message queue").
//
// Phase 1: no load shedding yet — every request is accepted, so RPC failures
// stay at zero. Shedding (and the failures it produces) arrives in a later phase.
package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed static/*
var staticFS embed.FS

const (
	workQueueKey    = "work:queue"
	completionChan  = "completions"
	latencyUntilKey = "chaos:latency_until_ms"
	chaosDuration   = 8 * time.Second
	sampleInterval  = 250 * time.Millisecond
)

type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
}

// snapshot is the server-side metric sample streamed to the browser. Goodput is
// deliberately absent: it is measured in the client, since that is where a
// request is observed to actually succeed.
type snapshot struct {
	StartWorkQPS float64 `json:"start_work_qps"`
	GetOpQPS     float64 `json:"get_op_qps"`
	RPCSuccessPS float64 `json:"rpc_success_ps"`
	RPCFailurePS float64 `json:"rpc_failure_ps"`
	InFlight     int64   `json:"in_flight"`
	QueueLen     int64   `json:"queue_len"`
	TsMs         int64   `json:"ts_ms"`
}

// metrics holds the raw atomic counters plus the latest published sample.
type metrics struct {
	startWork  atomic.Int64
	getOp      atomic.Int64
	rpcSuccess atomic.Int64
	rpcFailure atomic.Int64
	inFlight   atomic.Int64
	latest     atomic.Pointer[snapshot]
}

type server struct {
	rdb  *redis.Client
	m    metrics
	done sync.Map // operation_id -> struct{}, populated from pub/sub
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	s := &server{rdb: rdb}

	go s.subscribeCompletions(context.Background())
	go s.runSampler(context.Background())

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("embed static: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))
	mux.HandleFunc("/api/start", s.handleStart)
	mux.HandleFunc("/api/op", s.handleGetOp)
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/chaos/latency", s.handleChaosLatency)

	listen := ":" + getenv("PORT", "8080")
	log.Printf("frontend listening on %s (redis=%s)", listen, addr)
	log.Fatal(http.ListenAndServe(listen, mux))
}

func (s *server) subscribeCompletions(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, completionChan)
	for msg := range sub.Channel() {
		s.done.Store(msg.Payload, struct{}{})
		s.m.inFlight.Add(-1)
	}
}

// runSampler converts the raw counters into per-second rates every tick and
// publishes a snapshot that every SSE client reads.
func (s *server) runSampler(ctx context.Context) {
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	dt := sampleInterval.Seconds()
	var lsw, lgo, lok, lerr int64
	for range t.C {
		sw, gop := s.m.startWork.Load(), s.m.getOp.Load()
		ok, er := s.m.rpcSuccess.Load(), s.m.rpcFailure.Load()
		qlen, _ := s.rdb.LLen(ctx, workQueueKey).Result()
		inflight := s.m.inFlight.Load()
		if inflight < 0 {
			inflight = 0
		}
		s.m.latest.Store(&snapshot{
			StartWorkQPS: float64(sw-lsw) / dt,
			GetOpQPS:     float64(gop-lgo) / dt,
			RPCSuccessPS: float64(ok-lok) / dt,
			RPCFailurePS: float64(er-lerr) / dt,
			InFlight:     inflight,
			QueueLen:     qlen,
			TsMs:         time.Now().UnixMilli(),
		})
		lsw, lgo, lok, lerr = sw, gop, ok, er
	}
}

// handleStart implements the StartWork RPC.
func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	s.m.startWork.Add(1)
	id := randHex()
	item := workItem{ID: id, EnqueuedMs: time.Now().UnixMilli()}
	b, _ := json.Marshal(item)
	if err := s.rdb.LPush(r.Context(), workQueueKey, b).Err(); err != nil {
		s.m.rpcFailure.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.m.rpcSuccess.Add(1)
	s.m.inFlight.Add(1)
	writeJSON(w, map[string]string{"operation_id": id})
}

// handleGetOp implements the GetOperation RPC.
func (s *server) handleGetOp(w http.ResponseWriter, r *http.Request) {
	s.m.getOp.Add(1)
	id := r.URL.Query().Get("id")
	status := "pending"
	if _, ok := s.done.Load(id); ok {
		status = "done"
	} else if v, _ := s.rdb.Get(r.Context(), "op:"+id).Result(); v == "done" {
		status = "done" // fallback if the pub/sub notification was missed
	}
	s.m.rpcSuccess.Add(1)
	writeJSON(w, map[string]string{"status": status})
}

// handleStream pushes the latest metric snapshot to the browser over SSE.
func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			snap := s.m.latest.Load()
			if snap == nil {
				continue
			}
			b, _ := json.Marshal(snap)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

// handleChaosLatency arms a short-lived 4x backend-latency spike by writing the
// deadline to Redis, where the backend picks it up.
func (s *server) handleChaosLatency(w http.ResponseWriter, r *http.Request) {
	until := time.Now().Add(chaosDuration).UnixMilli()
	if err := s.rdb.Set(r.Context(), latencyUntilKey, until, 2*chaosDuration).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"until_ms": until, "duration_ms": chaosDuration.Milliseconds()})
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
