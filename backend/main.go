// Command backend consumes work items from the Redis work queue, simulates
// doing the work, and publishes a completion notification so the frontend can
// answer GetOperation calls.
//
// Phase 1: still FIFO with a fixed worker pool, but work latency can be spiked
// 4x for a short window via a Redis flag the frontend sets (the "4x latency"
// chaos button). The remaining knobs arrive in later phases.
package main

import (
	"context"
	"encoding/json"
	"log"
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
	latencyUntilKey = "chaos:latency_until_ms"
	latencyMult     = 4
)

// latencyUntilMs is a local cache of the latency-spike deadline (unix ms) so
// workers don't hit Redis on every item. Refreshed by refreshChaos.
var latencyUntilMs atomic.Int64

type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	concurrency := getenvInt("CONCURRENCY", 8)
	baseLatency := time.Duration(getenvInt("WORK_LATENCY_MS", 100)) * time.Millisecond

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	log.Printf("backend starting: concurrency=%d base_latency=%s redis=%s", concurrency, baseLatency, addr)

	go refreshChaos(ctx, rdb)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, rdb, baseLatency)
		}()
	}
	wg.Wait()
}

// refreshChaos keeps the local view of the latency-spike window up to date.
func refreshChaos(ctx context.Context, rdb *redis.Client) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		v, err := rdb.Get(ctx, latencyUntilKey).Int64()
		if err == redis.Nil {
			latencyUntilMs.Store(0)
			continue
		}
		if err != nil {
			continue // transient; keep the previous value
		}
		latencyUntilMs.Store(v)
	}
}

func runWorker(ctx context.Context, rdb *redis.Client, baseLatency time.Duration) {
	for {
		// FIFO: the frontend LPUSHes, so the oldest item is at the tail (BRPOP).
		res, err := rdb.BRPop(ctx, 5*time.Second, workQueueKey).Result()
		if err == redis.Nil {
			continue // no work within the timeout; loop and block again
		}
		if err != nil {
			log.Printf("brpop error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var item workItem
		if err := json.Unmarshal([]byte(res[1]), &item); err != nil {
			log.Printf("bad work item: %v", err)
			continue
		}

		// Simulate doing the work — 4x slower while a latency spike is active.
		latency := baseLatency
		if time.Now().UnixMilli() < latencyUntilMs.Load() {
			latency = baseLatency * latencyMult
		}
		time.Sleep(latency)

		// Record the result and notify the frontend (the "message queue").
		if err := rdb.Set(ctx, "op:"+item.ID, "done", time.Hour).Err(); err != nil {
			log.Printf("set error: %v", err)
		}
		rdb.Publish(ctx, completionChan, item.ID)
	}
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
