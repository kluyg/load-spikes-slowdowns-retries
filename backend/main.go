// Command backend consumes work items and simulates doing them, publishing a
// completion so the frontend can answer GetOperation calls.
//
// Every user gets an isolated "room": a namespace of Redis keys plus its own
// dedicated worker pool, so two people driving the deployed system concurrently
// never share a queue, config, metrics, or — crucially — capacity. The demo's
// headline number (capacity = workers / latency ≈ 80 ops/s) only holds if each
// room owns its own workers, so we spin up CONCURRENCY workers *per active
// room* rather than one global pool.
//
// A room is "active" while the frontend keeps its score fresh in the `rooms`
// sorted set (it does so on every RPC and on each metric-stream tick). This
// manager polls that set: a freshly-seen room gets a pool; a room whose score
// goes stale gets its pool torn down. The frontend deletes a room's keys a few
// seconds *after* we stop its workers (see roomIdleStop vs. the frontend's GC
// window), so workers never write to keys that are about to be deleted.
//
// Each pool's behavior is live-tunable via the room's config hash:
//   - queue discipline:  FIFO (serve oldest) vs LIFO (serve newest)
//   - deadline propagation: none / drop-if-expired / drop-if-no-margin
//   - latency distribution: uniform vs long-tail (mean-preserving lognormal)
//
// It also honors a short-lived 4x latency spike. This reproduces the dynamics
// from https://strebkov.dev/posts/shed-your-load/ : under overload, FIFO serves
// work whose clients already gave up (throughput stays flat, goodput collapses),
// LIFO serves the freshest work, and a deadline margin refuses work it can't
// finish in time rather than discovering the waste afterward.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	roomsKey      = "rooms" // ZSET: room id -> last-activity unix ms (maintained by the frontend)
	latencyMult   = 4
	longtailSigma = 0.9 // spread of the long-tail lognormal
	// A room stops being served roomIdleStop after its last activity. This is
	// deliberately shorter than the frontend's key-GC window so a room's workers
	// are gone before its keys are deleted.
	roomIdleStop = 15 * time.Second
	managePeriod = 500 * time.Millisecond
	// A finished op's "done" record only matters while its client is still
	// polling, i.e. until the op's deadline. Keep it that long plus doneBuffer
	// (generous slack for clock skew between browser and server, and for a late
	// final poll), never longer than doneMaxTTL however far out the client-
	// supplied deadline claims to be.
	doneBuffer = 60 * time.Second
	doneMaxTTL = time.Hour
)

// roomKey builds the Redis key for a per-room datum. Everything a room touches
// lives under "room:<id>:" so rooms never collide.
func roomKey(room, name string) string { return "room:" + room + ":" + name }

// Config holds the live-tunable backend knobs.
type Config struct {
	QueueMode    string `json:"queue_mode"`    // "FIFO" | "LIFO"
	DeadlineMode string `json:"deadline_mode"` // "none" | "expired" | "margin"
	MarginMs     int64  `json:"margin_ms"`
	LatencyMode  string `json:"latency_mode"` // "uniform" | "longtail"
	LatencyMs    int64  `json:"latency_ms"`
}

func defaultConfig() Config {
	return Config{QueueMode: "FIFO", DeadlineMode: "none", MarginMs: 70, LatencyMode: "uniform", LatencyMs: 50}
}

type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
	DeadlineMs int64  `json:"deadline_ms"` // absolute client deadline (unix ms)
}

type completion struct {
	ID     string `json:"id"`
	Status string `json:"status"` // "done" | "dropped"
	// TTLMs is how long a "done" is worth remembering (see doneTTL). The
	// frontend expires its in-memory record on the same schedule as the Redis
	// marker.
	TTLMs int64 `json:"ttl_ms,omitempty"`
}

// doneTTL is how long to remember that an op finished: until its deadline plus
// doneBuffer. An op with no deadline, or one already past it, gets the buffer
// alone.
func doneTTL(deadlineMs, nowMs int64) time.Duration {
	ttl := doneBuffer
	if left := time.Duration(deadlineMs-nowMs) * time.Millisecond; deadlineMs > 0 && left > 0 {
		ttl += left
	}
	if ttl > doneMaxTTL {
		ttl = doneMaxTTL
	}
	return ttl
}

// roomPool is one room's dedicated slice of capacity: CONCURRENCY workers plus a
// refresher, all under ctx. cfgPtr/latencyUntilMs cache the room's live state so
// the workers never hit Redis on the hot path.
//
// Each pool owns its own Redis client. An idle worker parks in a blocking pop,
// holding a connection for the whole wait, so a client shared across rooms runs
// out of connections once enough rooms are open (go-redis defaults to
// 10 x GOMAXPROCS, i.e. 10 on a 1-vCPU host) — and then idle rooms starve busy
// ones, which is exactly the cross-room interference rooms exist to prevent.
type roomPool struct {
	cancel         context.CancelFunc
	rdb            *redis.Client
	cfgPtr         atomic.Pointer[Config]
	latencyUntilMs atomic.Int64
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	concurrency := getenvInt("CONCURRENCY", 4)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	log.Printf("backend starting: concurrency=%d/room redis=%s", concurrency, addr)
	manageRooms(ctx, rdb, addr, concurrency)
}

// manageRooms polls the `rooms` set and keeps one pool alive per active room:
// start a pool the first time a room appears, cancel it once the room goes stale.
func manageRooms(ctx context.Context, rdb *redis.Client, addr string, concurrency int) {
	pools := map[string]*roomPool{}
	t := time.NewTicker(managePeriod)
	defer t.Stop()
	for {
		cutoff := time.Now().Add(-roomIdleStop).UnixMilli()
		active, err := rdb.ZRangeByScore(ctx, roomsKey, &redis.ZRangeBy{
			Min: strconv.FormatInt(cutoff, 10),
			Max: "+inf",
		}).Result()
		if err != nil {
			log.Printf("rooms poll error: %v", err)
			<-t.C
			continue
		}

		activeSet := make(map[string]bool, len(active))
		for _, room := range active {
			activeSet[room] = true
			if pools[room] == nil {
				pools[room] = startPool(ctx, addr, room, concurrency)
				log.Printf("room %s: pool up (%d workers)", room, concurrency)
			}
		}
		for room, p := range pools {
			if !activeSet[room] {
				p.stop()
				delete(pools, room)
				log.Printf("room %s: pool down (idle)", room)
			}
		}
		<-t.C
	}
}

func startPool(parent context.Context, addr, room string, concurrency int) *roomPool {
	ctx, cancel := context.WithCancel(parent)
	// One connection per worker (each may be parked in a blocking pop) plus
	// headroom for the refresher, so nothing in this room ever waits on the pool.
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: concurrency + 2})
	p := &roomPool{cancel: cancel, rdb: rdb}
	c := defaultConfig()
	p.cfgPtr.Store(&c)

	go refreshRoomState(ctx, rdb, room, p)
	for i := 0; i < concurrency; i++ {
		go runWorker(ctx, rdb, room, p)
	}
	return p
}

// stop tears the pool down. Closing the client also breaks workers out of a
// blocking pop immediately; otherwise one could sit there for seconds after the
// cancel and swallow an item enqueued for the room's next pool.
func (p *roomPool) stop() {
	p.cancel()
	p.rdb.Close()
}

// refreshRoomState keeps a pool's cached config + latency-spike deadline current
// so its workers don't hit Redis per item. It exits when the pool is cancelled.
func refreshRoomState(ctx context.Context, rdb *redis.Client, room string, p *roomPool) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		cfg := loadConfig(ctx, rdb, room)
		p.cfgPtr.Store(&cfg)

		if v, err := rdb.Get(ctx, roomKey(room, "chaos:latency_until_ms")).Int64(); err == redis.Nil {
			p.latencyUntilMs.Store(0)
		} else if err == nil {
			p.latencyUntilMs.Store(v)
		}

		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func loadConfig(ctx context.Context, rdb *redis.Client, room string) Config {
	c := defaultConfig()
	m, err := rdb.HGetAll(ctx, roomKey(room, "config")).Result()
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
	if n, err := strconv.ParseInt(m["margin_ms"], 10, 64); err == nil {
		c.MarginMs = n
	}
	if n, err := strconv.ParseInt(m["latency_ms"], 10, 64); err == nil && n > 0 {
		c.LatencyMs = n
	}
	return c
}

func runWorker(ctx context.Context, rdb *redis.Client, room string, p *roomPool) {
	queueKey := roomKey(room, "work:queue")
	inflightKey := roomKey(room, "metric:inflight")
	throughputKey := roomKey(room, "metric:throughput_total")
	for {
		cfg := *p.cfgPtr.Load()

		// FIFO: frontend LPUSHes (head), so BRPOP takes the oldest from the tail.
		// LIFO: BLPOP takes the newest from the head. LIFO is a no-op until the
		// queue backs up — then it serves the work most likely still wanted.
		var res []string
		var err error
		if cfg.QueueMode == "LIFO" {
			res, err = rdb.BLPop(ctx, 5*time.Second, queueKey).Result()
		} else {
			res, err = rdb.BRPop(ctx, 5*time.Second, queueKey).Result()
		}
		if ctx.Err() != nil {
			return // pool cancelled (room went idle)
		}
		if err == redis.Nil {
			continue
		}
		if err != nil {
			log.Printf("pop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Every popped item was counted in-flight by the frontend at enqueue;
		// decrement the durable gauge once it is resolved here (done/dropped/bad).
		var item workItem
		if err := json.Unmarshal([]byte(res[1]), &item); err != nil {
			log.Printf("bad work item: %v", err)
			rdb.Decr(ctx, inflightKey)
			continue
		}

		// Deadline propagation: decide whether the work is still worth doing.
		now := time.Now().UnixMilli()
		drop := false
		switch cfg.DeadlineMode {
		case "expired":
			// Already past deadline: the client has certainly given up.
			drop = item.DeadlineMs > 0 && now > item.DeadlineMs
		case "margin":
			// Won't finish with margin to spare: refuse now rather than waste
			// a worker discovering it can't make the deadline.
			drop = item.DeadlineMs > 0 && now+cfg.MarginMs > item.DeadlineMs
		}
		if drop {
			rdb.Decr(ctx, inflightKey)
			publish(ctx, rdb, room, item.ID, "dropped", 0)
			continue
		}

		time.Sleep(sampleLatency(cfg, p))
		if ctx.Err() != nil {
			return // pool cancelled mid-item; the room's keys are about to be deleted
		}

		ttl := doneTTL(item.DeadlineMs, time.Now().UnixMilli())
		if err := rdb.Set(ctx, roomKey(room, "op:"+item.ID), "done", ttl).Err(); err != nil {
			log.Printf("set error: %v", err)
		}
		rdb.Incr(ctx, throughputKey)
		rdb.Decr(ctx, inflightKey)
		publish(ctx, rdb, room, item.ID, "done", ttl)
	}
}

// sampleLatency returns the simulated service time, including a long-tail draw
// and the 4x spike multiplier when active.
func sampleLatency(cfg Config, p *roomPool) time.Duration {
	base := float64(cfg.LatencyMs)
	ms := base
	if cfg.LatencyMode == "longtail" {
		// Mean-preserving lognormal: same average service time, fat tail. This
		// keeps capacity (= workers / mean) fixed while p99 blows out.
		ms = base * math.Exp(longtailSigma*rand.NormFloat64()-longtailSigma*longtailSigma/2)
	}
	d := time.Duration(ms) * time.Millisecond
	if time.Now().UnixMilli() < p.latencyUntilMs.Load() {
		d *= latencyMult
	}
	return d
}

func publish(ctx context.Context, rdb *redis.Client, room, id, status string, ttl time.Duration) {
	b, _ := json.Marshal(completion{ID: id, Status: status, TTLMs: ttl.Milliseconds()})
	rdb.Publish(ctx, roomKey(room, "completions"), b)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
