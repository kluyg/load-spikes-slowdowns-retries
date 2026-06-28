// Command backend consumes work items from the Redis work queue, simulates
// doing the work, and publishes a completion notification so the frontend can
// answer GetOperation calls.
//
// Phase 0: FIFO only, fixed work latency, fixed worker-pool concurrency. The
// knobs (LIFO, bounded queue, deadline propagation, latency distributions)
// arrive in later phases.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	workQueueKey   = "work:queue"
	completionChan = "completions"
)

// workItem is the unit of work the frontend enqueues and the backend consumes.
type workItem struct {
	ID         string `json:"id"`
	EnqueuedMs int64  `json:"enqueued_ms"`
}

func main() {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	concurrency := getenvInt("CONCURRENCY", 8)
	latency := time.Duration(getenvInt("WORK_LATENCY_MS", 100)) * time.Millisecond

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()

	log.Printf("backend starting: concurrency=%d latency=%s redis=%s", concurrency, latency, addr)

	// A fixed pool of workers. Service capacity ≈ concurrency / latency; this is
	// the ceiling that arriving load has to stay under to avoid a backlog.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, rdb, latency)
		}()
	}
	wg.Wait()
}

func runWorker(ctx context.Context, rdb *redis.Client, latency time.Duration) {
	for {
		// FIFO: the frontend LPUSHes, so the oldest item is at the tail (BRPOP).
		// Switching to BLPOP later gives LIFO for free.
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

		// Simulate doing the work.
		time.Sleep(latency)

		// Record the result and notify the frontend (the "message queue").
		// The SET is the durable source of truth; the PUBLISH is the fast path.
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
