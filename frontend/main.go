// Command frontend is the edge of the system. It serves the single-page UI
// (which also hosts the browser-based client tier) and exposes:
//
//	POST /api/start          StartWork    — enqueue a work item, return an op id
//	GET  /api/op?id=         GetOperation — report whether that op has completed
//	GET  /api/stream         SSE stream of server-side metrics
//	GET  /api/config         current live knobs
//	POST /api/config         update live knobs (stored in a Redis hash)
//	POST /api/chaos/latency  trigger a short 4x backend-latency spike
//
// Completions are learned by subscribing to the Redis pub/sub channel the
// backend publishes to (the "message queue").
//
// Phase 2a (backend knobs): the frontend owns the "queue max size" knob (it
// rejects StartWork when the queue is full) and propagates each client's
// deadline into the work item. Load shedding and client retries arrive later.
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
	"strconv"
	"strings"
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
	configKey       = "config"
	latencyUntilKey = "chaos:latency_until_ms"
	inflightKey     = "metric:inflight"        // durable gauge (we INCR, backend DECRs)
	throughputKey   = "metric:throughput_total" // durable counter of work done
	clearEpochKey   = "clear_epoch_ms"          // last operator queue-clear time
	chaosDuration   = 8 * time.Second
	sampleInterval  = 250 * time.Millisecond
)

// clearQueueScript atomically drops the backlog, reconciles the in-flight gauge
// (the cleared items will never be resolved by the backend), and stamps the
// clear epoch so GetOperation can 404 the orphaned operations.
var clearQueueScript = redis.NewScript(`
local n = redis.call('LLEN', KEYS[1])
redis.call('DEL', KEYS[1])
redis.call('DECRBY', KEYS[2], n)
redis.call('SET', KEYS[3], ARGV[1])
return n
`)

type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
	DeadlineMs int64  `json:"deadline_ms"`
}

type completion struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// Config holds the live-tunable knobs. The frontend acts on QueueMax; the rest
// are owned by the backend but stored/served here so the UI has one place to
// read and write them.
type Config struct {
	QueueMode    string `json:"queue_mode"`
	QueueMax     int64  `json:"queue_max"` // 0 = unbounded
	DeadlineMode string `json:"deadline_mode"`
	MarginMs     int64  `json:"margin_ms"`
	LatencyMode  string `json:"latency_mode"`
	LatencyMs    int64  `json:"latency_ms"`
}

func defaultConfig() Config {
	return Config{QueueMode: "FIFO", QueueMax: 0, DeadlineMode: "none", MarginMs: 70, LatencyMode: "uniform", LatencyMs: 50}
}

// snapshot is the server-side metric sample streamed to the browser. Goodput is
// absent on purpose: it's measured in the client, where success is observed.
type snapshot struct {
	StartWorkQPS float64 `json:"start_work_qps"`
	GetOpQPS     float64 `json:"get_op_qps"`
	RPCSuccessPS float64 `json:"rpc_success_ps"`
	RPCFailurePS float64 `json:"rpc_failure_ps"`
	ThroughputPS float64 `json:"throughput_ps"` // backend completions/s (work done)
	InFlight     int64   `json:"in_flight"`
	QueueLen     int64   `json:"queue_len"`
	TsMs         int64   `json:"ts_ms"`
}

// metrics holds frontend-local request counters. In-flight and throughput live
// in Redis (durable counters) rather than here, so they survive pub/sub loss.
type metrics struct {
	startWork  atomic.Int64
	getOp      atomic.Int64
	rpcSuccess atomic.Int64
	rpcFailure atomic.Int64
	latest     atomic.Pointer[snapshot]
}

type server struct {
	rdb          *redis.Client
	m            metrics
	cfgPtr       atomic.Pointer[Config]
	clearEpochMs atomic.Int64 // ops enqueued before this were cleared -> 404
	done         sync.Map
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	s := &server{rdb: rdb}

	go s.subscribeCompletions(context.Background())
	go s.runSampler(context.Background())
	go s.refreshConfig(context.Background())

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("embed static: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(static)))
	mux.HandleFunc("/api/start", s.handleStart)
	mux.HandleFunc("/api/op", s.handleGetOp)
	mux.HandleFunc("/api/stream", s.handleStream)
	mux.HandleFunc("/api/metrics", s.handleMetrics)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/chaos/latency", s.handleChaosLatency)
	mux.HandleFunc("/api/admin/clear-queue", s.handleClearQueue)

	listen := ":" + getenv("PORT", "8080")
	log.Printf("frontend listening on %s (redis=%s)", listen, addr)
	log.Fatal(http.ListenAndServe(listen, mux))
}

func (s *server) currentConfig() Config {
	if c := s.cfgPtr.Load(); c != nil {
		return *c
	}
	return defaultConfig()
}

func (s *server) refreshConfig(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		cfg := s.loadConfig(ctx)
		s.cfgPtr.Store(&cfg)
		if ce, err := s.rdb.Get(ctx, clearEpochKey).Int64(); err == nil {
			s.clearEpochMs.Store(ce)
		}
		<-t.C
	}
}

func (s *server) loadConfig(ctx context.Context) Config {
	c := defaultConfig()
	m, err := s.rdb.HGetAll(ctx, configKey).Result()
	if err != nil {
		return c
	}
	if v := m["queue_mode"]; v != "" {
		c.QueueMode = v
	}
	if v := m["deadline_mode"]; v != "" {
		c.DeadlineMode = v
	}
	if v := m["latency_mode"]; v != "" {
		c.LatencyMode = v
	}
	if n, err := strconv.ParseInt(m["queue_max"], 10, 64); err == nil {
		c.QueueMax = n
	}
	if n, err := strconv.ParseInt(m["margin_ms"], 10, 64); err == nil {
		c.MarginMs = n
	}
	if n, err := strconv.ParseInt(m["latency_ms"], 10, 64); err == nil && n > 0 {
		c.LatencyMs = n
	}
	return c
}

// subscribeCompletions feeds only the GetOperation fast-path. Pub/sub is
// at-most-once, so it is never used as a source of truth: the durable "op:<id>"
// SET (checked as a fallback in GetOperation) and the Redis metric counters are.
func (s *server) subscribeCompletions(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, completionChan)
	for msg := range sub.Channel() {
		var c completion
		if err := json.Unmarshal([]byte(msg.Payload), &c); err != nil {
			continue
		}
		if c.Status == "done" {
			s.done.Store(c.ID, struct{}{})
		}
	}
}

func (s *server) runSampler(ctx context.Context) {
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	dt := sampleInterval.Seconds()
	var lsw, lgo, lok, lerr, ldone int64
	for range t.C {
		sw, gop := s.m.startWork.Load(), s.m.getOp.Load()
		ok, er := s.m.rpcSuccess.Load(), s.m.rpcFailure.Load()
		qlen, _ := s.rdb.LLen(ctx, workQueueKey).Result()
		// Durable gauge + counter (immune to pub/sub loss).
		done := getInt(ctx, s.rdb, throughputKey)
		inflight := getInt(ctx, s.rdb, inflightKey)
		if inflight < 0 {
			inflight = 0
		}
		s.m.latest.Store(&snapshot{
			StartWorkQPS: float64(sw-lsw) / dt,
			GetOpQPS:     float64(gop-lgo) / dt,
			RPCSuccessPS: float64(ok-lok) / dt,
			RPCFailurePS: float64(er-lerr) / dt,
			ThroughputPS: float64(done-ldone) / dt,
			InFlight:     inflight,
			QueueLen:     qlen,
			TsMs:         time.Now().UnixMilli(),
		})
		lsw, lgo, lok, lerr, ldone = sw, gop, ok, er, done
	}
}

// handleStart implements the StartWork RPC.
func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	s.m.startWork.Add(1)

	// Queue-max knob: reject when the backlog is full (enqueue-time shedding).
	cfg := s.currentConfig()
	if cfg.QueueMax > 0 {
		if n, _ := s.rdb.LLen(r.Context(), workQueueKey).Result(); n >= cfg.QueueMax {
			s.m.rpcFailure.Add(1)
			http.Error(w, "queue full", http.StatusServiceUnavailable)
			return
		}
	}

	deadlineMs, _ := strconv.ParseInt(r.URL.Query().Get("deadline_ms"), 10, 64)
	nowMs := time.Now().UnixMilli()
	// The id carries its enqueue time so GetOperation can tell whether an op
	// predates the last queue clear (and is therefore gone) without any lookup.
	id := strconv.FormatInt(nowMs, 10) + "-" + randHex()
	item := workItem{ID: id, EnqueuedMs: nowMs, DeadlineMs: deadlineMs}
	b, _ := json.Marshal(item)
	if err := s.rdb.LPush(r.Context(), workQueueKey, b).Err(); err != nil {
		s.m.rpcFailure.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.m.rpcSuccess.Add(1)
	s.rdb.Incr(r.Context(), inflightKey) // durable in-flight gauge; backend DECRs on resolve
	writeJSON(w, map[string]string{"operation_id": id})
}

// handleGetOp implements the GetOperation RPC.
func (s *server) handleGetOp(w http.ResponseWriter, r *http.Request) {
	s.m.getOp.Add(1)
	s.m.rpcSuccess.Add(1)
	id := r.URL.Query().Get("id")

	if _, ok := s.done.Load(id); ok {
		writeJSON(w, map[string]string{"status": "done"})
		return
	}
	if v, _ := s.rdb.Get(r.Context(), "op:"+id).Result(); v == "done" {
		writeJSON(w, map[string]string{"status": "done"}) // durable fallback
		return
	}
	// Not done. If it was enqueued before the last operator queue-clear, it was
	// thrown away — tell the client so it can issue a fresh operation.
	if ce := s.clearEpochMs.Load(); ce > 0 {
		if ts := opTimestamp(id); ts > 0 && ts < ce {
			http.Error(w, "operation not found", http.StatusNotFound)
			return
		}
	}
	writeJSON(w, map[string]string{"status": "pending"})
}

// opTimestamp extracts the enqueue time encoded as the id's "<ms>-..." prefix.
func opTimestamp(id string) int64 {
	if i := strings.IndexByte(id, '-'); i > 0 {
		if n, err := strconv.ParseInt(id[:i], 10, 64); err == nil {
			return n
		}
	}
	return 0
}

// handleClearQueue is the operator "fix it" lever: drop the entire backlog. The
// orphaned operations will 404 on their next poll, and clients retry fresh.
func (s *server) handleClearQueue(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UnixMilli()
	n, err := clearQueueScript.Run(r.Context(), s.rdb,
		[]string{workQueueKey, inflightKey, clearEpochKey}, now).Int64()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.clearEpochMs.Store(now)
	writeJSON(w, map[string]any{"cleared": n})
}

// handleConfig serves and updates the live knobs.
func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, s.currentConfig())
		return
	}
	var c Config
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.rdb.HSet(r.Context(), configKey, map[string]any{
		"queue_mode":    c.QueueMode,
		"queue_max":     c.QueueMax,
		"deadline_mode": c.DeadlineMode,
		"margin_ms":     c.MarginMs,
		"latency_mode":  c.LatencyMode,
		"latency_ms":    c.LatencyMs,
	}).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.cfgPtr.Store(&c) // reflect immediately; refresher keeps it in sync
	writeJSON(w, c)
}

// handleMetrics returns the latest snapshot as plain JSON (handy for scripting).
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if snap := s.m.latest.Load(); snap != nil {
		writeJSON(w, snap)
		return
	}
	writeJSON(w, snapshot{})
}

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

func (s *server) handleChaosLatency(w http.ResponseWriter, r *http.Request) {
	until := time.Now().Add(chaosDuration).UnixMilli()
	if err := s.rdb.Set(r.Context(), latencyUntilKey, until, 2*chaosDuration).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"until_ms": until, "duration_ms": chaosDuration.Milliseconds()})
}

func getInt(ctx context.Context, rdb *redis.Client, key string) int64 {
	n, err := rdb.Get(ctx, key).Int64()
	if err != nil {
		return 0 // missing key (e.g. after flush) reads as zero
	}
	return n
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
