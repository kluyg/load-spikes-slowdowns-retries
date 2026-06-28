// Command backend consumes work items from the Redis work queue, simulates
// doing the work, and publishes a completion notification so the frontend can
// answer GetOperation calls.
//
// Phase 2a (backend knobs) — live-tunable via a Redis config hash:
//   - queue discipline:  FIFO (serve oldest) vs LIFO (serve newest)
//   - deadline propagation: none / drop-if-expired / drop-if-no-margin
//   - latency distribution: uniform vs long-tail (mean-preserving lognormal)
//
// Plus the short-lived 4x latency spike from Phase 1. This reproduces the
// dynamics from https://strebkov.dev/posts/shed-your-load/ : under overload,
// FIFO serves work whose clients already gave up (throughput stays flat, goodput
// collapses), LIFO serves the freshest work, and a deadline margin refuses work
// it can't finish in time rather than discovering the waste afterward.
package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	workQueueKey    = "work:queue"
	completionChan  = "completions"
	configKey       = "config"
	latencyUntilKey = "chaos:latency_until_ms"
	latencyMult     = 4
	longtailSigma   = 0.9 // spread of the long-tail lognormal
)

// latencyUntilMs caches the latency-spike deadline; cfgPtr caches the live knobs.
// Both are refreshed by refreshState so the hot path never blocks on Redis.
var (
	latencyUntilMs atomic.Int64
	cfgPtr         atomic.Pointer[Config]
)

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

func currentConfig() Config {
	if c := cfgPtr.Load(); c != nil {
		return *c
	}
	return defaultConfig()
}

type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
	DeadlineMs int64  `json:"deadline_ms"` // absolute client deadline (unix ms)
}

type completion struct {
	ID     string `json:"id"`
	Status string `json:"status"` // "done" | "dropped"
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	concurrency := getenvInt("CONCURRENCY", 4)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	log.Printf("backend starting: concurrency=%d redis=%s", concurrency, addr)

	go refreshState(ctx, rdb)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, rdb)
		}()
	}
	wg.Wait()
}

// refreshState keeps the local view of config + latency spike up to date so the
// workers don't hit Redis on every item.
func refreshState(ctx context.Context, rdb *redis.Client) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		cfg := loadConfig(ctx, rdb)
		cfgPtr.Store(&cfg)

		if v, err := rdb.Get(ctx, latencyUntilKey).Int64(); err == redis.Nil {
			latencyUntilMs.Store(0)
		} else if err == nil {
			latencyUntilMs.Store(v)
		}
		<-t.C
	}
}

func loadConfig(ctx context.Context, rdb *redis.Client) Config {
	c := defaultConfig()
	m, err := rdb.HGetAll(ctx, configKey).Result()
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

func runWorker(ctx context.Context, rdb *redis.Client) {
	for {
		cfg := currentConfig()

		// FIFO: frontend LPUSHes (head), so BRPOP takes the oldest from the tail.
		// LIFO: BLPOP takes the newest from the head. LIFO is a no-op until the
		// queue backs up — then it serves the work most likely still wanted.
		var res []string
		var err error
		if cfg.QueueMode == "LIFO" {
			res, err = rdb.BLPop(ctx, 5*time.Second, workQueueKey).Result()
		} else {
			res, err = rdb.BRPop(ctx, 5*time.Second, workQueueKey).Result()
		}
		if err == redis.Nil {
			continue
		}
		if err != nil {
			log.Printf("pop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var item workItem
		if err := json.Unmarshal([]byte(res[1]), &item); err != nil {
			log.Printf("bad work item: %v", err)
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
			publish(ctx, rdb, item.ID, "dropped")
			continue
		}

		time.Sleep(sampleLatency(cfg))

		if err := rdb.Set(ctx, "op:"+item.ID, "done", time.Hour).Err(); err != nil {
			log.Printf("set error: %v", err)
		}
		publish(ctx, rdb, item.ID, "done")
	}
}

// sampleLatency returns the simulated service time, including a long-tail draw
// and the 4x spike multiplier when active.
func sampleLatency(cfg Config) time.Duration {
	base := float64(cfg.LatencyMs)
	ms := base
	if cfg.LatencyMode == "longtail" {
		// Mean-preserving lognormal: same average service time, fat tail. This
		// keeps capacity (= workers / mean) fixed while p99 blows out.
		ms = base * math.Exp(longtailSigma*rand.NormFloat64()-longtailSigma*longtailSigma/2)
	}
	d := time.Duration(ms) * time.Millisecond
	if time.Now().UnixMilli() < latencyUntilMs.Load() {
		d *= latencyMult
	}
	return d
}

func publish(ctx context.Context, rdb *redis.Client, id, status string) {
	b, _ := json.Marshal(completion{ID: id, Status: status})
	rdb.Publish(ctx, completionChan, b)
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
