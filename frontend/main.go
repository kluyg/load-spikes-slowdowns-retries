// Command frontend is the edge of the system. It serves the single-page UI
// (which also hosts the browser-based client tier) and exposes:
//
//	POST /api/start          StartWork    — enqueue a work item, return an op id
//	GET  /api/op?id=         GetOperation — report whether that op has completed
//	GET  /api/stream             SSE stream of server-side metrics
//	GET  /api/metrics            latest metric snapshot as JSON
//	GET  /api/config             current live knobs
//	POST /api/config             update live knobs (stored in a Redis hash)
//	POST /api/chaos/latency      trigger a short 4x backend-latency spike
//	POST /api/admin/clear-queue  operator lever: drop the whole backlog
//
// Every request carries a `room` id, and each room is fully isolated: its own
// work queue, config, metrics, completion bus, and (in the backend) its own
// worker pool. This is what lets two people drive the deployed system at once
// without their graphs — or their capacity — bleeding together. All of a room's
// state lives under Redis keys prefixed "room:<id>:", and per-room in-process
// state (request counters, config cache, token bucket, seen completions) lives
// in a roomState looked up by id.
//
// A room is kept alive by touching the `rooms` sorted set on every request and
// on each metric-stream tick; a janitor deletes an idle room's keys, and the
// backend tears down its pool. Completions are learned by pattern-subscribing to
// every room's pub/sub channel.
//
// The frontend applies load shedding (fixed max QPS or in-flight quota),
// enforces the queue-max limit, and propagates each client's deadline into the
// work item.
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
	"regexp"
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
	roomsKey       = "rooms" // ZSET: room id -> last-activity unix ms
	chaosDuration  = 8 * time.Second
	sampleInterval = 250 * time.Millisecond
	// An idle room's keys are deleted roomIdleGC after its last activity. This
	// is longer than the backend's roomIdleStop so a room's workers are gone
	// before we delete the keys they might otherwise write to.
	roomIdleGC = 20 * time.Second
)

// roomRe bounds room ids so they're safe to splice into Redis keys.
var roomRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// roomKey builds the Redis key for a per-room datum, matching the backend.
func roomKey(room, name string) string { return "room:" + room + ":" + name }

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
	// Frontend load shedding (frontend-only knobs).
	ShedMode      string `json:"shed_mode"`      // none | qps | inflight
	MaxQPS        int64  `json:"max_qps"`        // qps mode: shared StartWork+GetOperation budget
	InflightQuota int64  `json:"inflight_quota"` // inflight mode: cap on admitted work
}

func defaultConfig() Config {
	return Config{
		QueueMode: "FIFO", QueueMax: 0, DeadlineMode: "none", MarginMs: 70,
		LatencyMode: "uniform", LatencyMs: 50,
		ShedMode: "none", MaxQPS: 0, InflightQuota: 0,
	}
}

// tokenBucket is a simple shared rate limiter for the fixed-max-QPS shed policy.
// Burst is one second's worth of tokens. The rate is passed in so it can change
// live with the config.
type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func (tb *tokenBucket) allow(rate float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	if tb.last.IsZero() {
		tb.last = now
	}
	tb.tokens += rate * now.Sub(tb.last).Seconds()
	tb.last = now
	if tb.tokens > rate {
		tb.tokens = rate // cap burst at ~1s
	}
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
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

// metrics holds a room's frontend-local request counters. In-flight and
// throughput live in Redis (durable counters) rather than here, so they survive
// pub/sub loss.
type metrics struct {
	startWork  atomic.Int64
	getOp      atomic.Int64
	rpcSuccess atomic.Int64
	rpcFailure atomic.Int64
	latest     atomic.Pointer[snapshot]
}

// roomState is one room's isolated in-process state. Everything here is scoped
// to a single room id; a room never sees another's counters, config, or bucket.
type roomState struct {
	m            metrics
	cfgPtr       atomic.Pointer[Config]
	clearEpochMs atomic.Int64 // ops enqueued before this were cleared -> 404
	bucket       tokenBucket  // rate limiter for this room's qps shed policy
	done         sync.Map     // completion fast-path (op id -> struct{})

	// sampler's previous cumulative values, for per-interval rates. Only the
	// sampler goroutine touches these, so they need no synchronization.
	lsw, lgo, lok, lerr, ldone int64
}

type server struct {
	rdb   *redis.Client
	mu    sync.Mutex
	rooms map[string]*roomState
}

// roomState returns the room's state, creating it (with default config) on first
// use. Safe for concurrent callers.
func (s *server) roomState(room string) *roomState {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := s.rooms[room]
	if rs == nil {
		rs = &roomState{}
		c := defaultConfig()
		rs.cfgPtr.Store(&c)
		s.rooms[room] = rs
	}
	return rs
}

func (rs *roomState) config() Config {
	if c := rs.cfgPtr.Load(); c != nil {
		return *c
	}
	return defaultConfig()
}

// roomParam validates and returns the request's room id, writing a 400 if it's
// missing or malformed.
func (s *server) roomParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	room := r.URL.Query().Get("room")
	if !roomRe.MatchString(room) {
		http.Error(w, "missing or invalid room", http.StatusBadRequest)
		return "", false
	}
	return room, true
}

// touch marks a room active so the backend keeps its pool up and the janitor
// leaves its keys alone.
func (s *server) touch(ctx context.Context, room string) {
	s.rdb.ZAdd(ctx, roomsKey, redis.Z{Score: float64(time.Now().UnixMilli()), Member: room})
}

// shouldShed decides whether to reject an RPC under the room's shed policy.
// The qps budget is shared by both RPCs; the in-flight quota gates only new
// work (StartWork), so polls for already-admitted work still get through.
func (s *server) shouldShed(ctx context.Context, room string, rs *roomState, cfg Config, isStart bool) bool {
	switch cfg.ShedMode {
	case "qps":
		return cfg.MaxQPS > 0 && !rs.bucket.allow(float64(cfg.MaxQPS))
	case "inflight":
		return isStart && cfg.InflightQuota > 0 &&
			getInt(ctx, s.rdb, roomKey(room, "metric:inflight")) >= cfg.InflightQuota
	}
	return false
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	s := &server{rdb: rdb, rooms: map[string]*roomState{}}

	go s.subscribeCompletions(context.Background())
	go s.runSampler(context.Background())
	go s.refreshConfig(context.Background())
	go s.gcRooms(context.Background())

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

// activeRooms returns a snapshot of the tracked rooms for background sweeps.
func (s *server) activeRooms() map[string]*roomState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*roomState, len(s.rooms))
	for k, v := range s.rooms {
		out[k] = v
	}
	return out
}

func (s *server) refreshConfig(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		for room, rs := range s.activeRooms() {
			cfg := s.loadConfig(ctx, room)
			rs.cfgPtr.Store(&cfg)
			if ce, err := s.rdb.Get(ctx, roomKey(room, "clear_epoch_ms")).Int64(); err == nil {
				rs.clearEpochMs.Store(ce)
			}
		}
	}
}

func (s *server) loadConfig(ctx context.Context, room string) Config {
	c := defaultConfig()
	m, err := s.rdb.HGetAll(ctx, roomKey(room, "config")).Result()
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
	if v := m["shed_mode"]; v != "" {
		c.ShedMode = v
	}
	if n, err := strconv.ParseInt(m["queue_max"], 10, 64); err == nil {
		c.QueueMax = n
	}
	if n, err := strconv.ParseInt(m["max_qps"], 10, 64); err == nil {
		c.MaxQPS = n
	}
	if n, err := strconv.ParseInt(m["inflight_quota"], 10, 64); err == nil {
		c.InflightQuota = n
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
// at-most-once, so it is never used as a source of truth: the durable
// "room:<id>:op:<id>" SET (checked as a fallback in GetOperation) and the Redis
// metric counters are. One pattern subscription covers every room's channel.
func (s *server) subscribeCompletions(ctx context.Context) {
	sub := s.rdb.PSubscribe(ctx, "room:*:completions")
	for msg := range sub.Channel() {
		room := roomFromChannel(msg.Channel)
		if room == "" {
			continue
		}
		var c completion
		if err := json.Unmarshal([]byte(msg.Payload), &c); err != nil {
			continue
		}
		if c.Status != "done" {
			continue
		}
		// Only record for rooms we're actively tracking; don't resurrect a
		// GC'd room from a late completion.
		s.mu.Lock()
		rs := s.rooms[room]
		s.mu.Unlock()
		if rs != nil {
			rs.done.Store(c.ID, struct{}{})
		}
	}
}

// roomFromChannel pulls "<id>" out of "room:<id>:completions".
func roomFromChannel(ch string) string {
	const pre, suf = "room:", ":completions"
	if strings.HasPrefix(ch, pre) && strings.HasSuffix(ch, suf) {
		return ch[len(pre) : len(ch)-len(suf)]
	}
	return ""
}

// runSampler computes a fresh per-room metric snapshot every interval. It walks
// only the rooms the frontend is tracking, so cost scales with live rooms.
func (s *server) runSampler(ctx context.Context) {
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	dt := sampleInterval.Seconds()
	for range t.C {
		for room, rs := range s.activeRooms() {
			sw, gop := rs.m.startWork.Load(), rs.m.getOp.Load()
			ok, er := rs.m.rpcSuccess.Load(), rs.m.rpcFailure.Load()
			qlen, _ := s.rdb.LLen(ctx, roomKey(room, "work:queue")).Result()
			// Durable gauge + counter (immune to pub/sub loss).
			done := getInt(ctx, s.rdb, roomKey(room, "metric:throughput_total"))
			inflight := getInt(ctx, s.rdb, roomKey(room, "metric:inflight"))
			if inflight < 0 {
				inflight = 0
			}
			rs.m.latest.Store(&snapshot{
				StartWorkQPS: float64(sw-rs.lsw) / dt,
				GetOpQPS:     float64(gop-rs.lgo) / dt,
				RPCSuccessPS: float64(ok-rs.lok) / dt,
				RPCFailurePS: float64(er-rs.lerr) / dt,
				ThroughputPS: float64(done-rs.ldone) / dt,
				InFlight:     inflight,
				QueueLen:     qlen,
				TsMs:         time.Now().UnixMilli(),
			})
			rs.lsw, rs.lgo, rs.lok, rs.lerr, rs.ldone = sw, gop, ok, er, done
		}
	}
}

// gcRooms deletes the keys of rooms that have gone idle and drops their
// in-process state. The backend has already stopped their workers by now (its
// idle window is shorter), so nothing is racing to rewrite these keys.
func (s *server) gcRooms(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-roomIdleGC).UnixMilli()
		stale, err := s.rdb.ZRangeByScore(ctx, roomsKey, &redis.ZRangeBy{
			Min: "-inf",
			Max: "(" + strconv.FormatInt(cutoff, 10),
		}).Result()
		if err != nil {
			continue
		}
		for _, room := range stale {
			s.rdb.ZRem(ctx, roomsKey, room)
			s.rdb.Del(ctx,
				roomKey(room, "work:queue"),
				roomKey(room, "config"),
				roomKey(room, "metric:inflight"),
				roomKey(room, "metric:throughput_total"),
				roomKey(room, "chaos:latency_until_ms"),
				roomKey(room, "clear_epoch_ms"),
			)
			s.mu.Lock()
			delete(s.rooms, room)
			s.mu.Unlock()
		}
	}
}

// handleStart implements the StartWork RPC.
func (s *server) handleStart(w http.ResponseWriter, r *http.Request) {
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	s.touch(r.Context(), room)
	rs := s.roomState(room)
	rs.m.startWork.Add(1)
	cfg := rs.config()

	// Load shedding: reject excess at the front door, cheaply, before any work
	// is enqueued. The client treats 429 as backpressure and does not retry.
	if s.shouldShed(r.Context(), room, rs, cfg, true) {
		rs.m.rpcFailure.Add(1)
		http.Error(w, "shed", http.StatusTooManyRequests)
		return
	}

	// Queue-max knob: reject when the backlog is full (enqueue-time shedding).
	if cfg.QueueMax > 0 {
		if n, _ := s.rdb.LLen(r.Context(), roomKey(room, "work:queue")).Result(); n >= cfg.QueueMax {
			rs.m.rpcFailure.Add(1)
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
	if err := s.rdb.LPush(r.Context(), roomKey(room, "work:queue"), b).Err(); err != nil {
		rs.m.rpcFailure.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rs.m.rpcSuccess.Add(1)
	s.rdb.Incr(r.Context(), roomKey(room, "metric:inflight")) // durable in-flight gauge; backend DECRs on resolve
	writeJSON(w, map[string]string{"operation_id": id})
}

// handleGetOp implements the GetOperation RPC.
func (s *server) handleGetOp(w http.ResponseWriter, r *http.Request) {
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	s.touch(r.Context(), room)
	rs := s.roomState(room)
	rs.m.getOp.Add(1)
	if s.shouldShed(r.Context(), room, rs, rs.config(), false) {
		rs.m.rpcFailure.Add(1)
		http.Error(w, "shed", http.StatusTooManyRequests)
		return
	}
	rs.m.rpcSuccess.Add(1)
	id := r.URL.Query().Get("id")

	if _, ok := rs.done.Load(id); ok {
		writeJSON(w, map[string]string{"status": "done"})
		return
	}
	if v, _ := s.rdb.Get(r.Context(), roomKey(room, "op:"+id)).Result(); v == "done" {
		writeJSON(w, map[string]string{"status": "done"}) // durable fallback
		return
	}
	// Not done. If it was enqueued before the last operator queue-clear, it was
	// thrown away — tell the client so it can issue a fresh operation.
	if ce := rs.clearEpochMs.Load(); ce > 0 {
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
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	s.touch(r.Context(), room)
	rs := s.roomState(room)
	now := time.Now().UnixMilli()
	n, err := clearQueueScript.Run(r.Context(), s.rdb, []string{
		roomKey(room, "work:queue"),
		roomKey(room, "metric:inflight"),
		roomKey(room, "clear_epoch_ms"),
	}, now).Int64()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rs.clearEpochMs.Store(now)
	writeJSON(w, map[string]any{"cleared": n})
}

// handleConfig serves and updates a room's live knobs.
func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	s.touch(r.Context(), room)
	rs := s.roomState(room)
	if r.Method == http.MethodGet {
		writeJSON(w, rs.config())
		return
	}
	var c Config
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.rdb.HSet(r.Context(), roomKey(room, "config"), map[string]any{
		"queue_mode":     c.QueueMode,
		"queue_max":      c.QueueMax,
		"deadline_mode":  c.DeadlineMode,
		"margin_ms":      c.MarginMs,
		"latency_mode":   c.LatencyMode,
		"latency_ms":     c.LatencyMs,
		"shed_mode":      c.ShedMode,
		"max_qps":        c.MaxQPS,
		"inflight_quota": c.InflightQuota,
	}).Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rs.cfgPtr.Store(&c) // reflect immediately; refresher keeps it in sync
	writeJSON(w, c)
}

// handleMetrics returns the room's latest snapshot as plain JSON (handy for
// scripting).
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	s.touch(r.Context(), room)
	if snap := s.roomState(room).m.latest.Load(); snap != nil {
		writeJSON(w, snap)
		return
	}
	writeJSON(w, snapshot{})
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	rs := s.roomState(room)
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			// An open stream keeps the room alive even when load is stopped.
			s.touch(r.Context(), room)
			snap := rs.m.latest.Load()
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
	room, ok := s.roomParam(w, r)
	if !ok {
		return
	}
	s.touch(r.Context(), room)
	until := time.Now().Add(chaosDuration).UnixMilli()
	if err := s.rdb.Set(r.Context(), roomKey(room, "chaos:latency_until_ms"), until, 2*chaosDuration).Err(); err != nil {
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
