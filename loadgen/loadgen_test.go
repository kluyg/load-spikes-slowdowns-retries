package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDueFollowsThePhases(t *testing.T) {
	sc := spikeScenario("x", "", nil) // 5 s @ 60, 10 s @ 280, 15 s @ 60
	for _, tc := range []struct {
		at   time.Duration
		want int
	}{
		{0, 0},
		{time.Second, 60},
		{5 * time.Second, 300},
		{6 * time.Second, 300 + 280},
		{15 * time.Second, 300 + 2800},
		{30 * time.Second, 300 + 2800 + 900},
		{time.Hour, 300 + 2800 + 900}, // never past the end of the run
	} {
		if got := sc.due(tc.at); got != tc.want {
			t.Errorf("due(%s) = %d, want %d", tc.at, got, tc.want)
		}
	}
}

func TestRetryDelay(t *testing.T) {
	half := func() float64 { return 0.5 }
	for _, tc := range []struct {
		strategy string
		attempt  int
		want     time.Duration
	}{
		{"immediate", 3, 0},
		{"backoff", 1, 100 * time.Millisecond},
		{"backoff", 4, 800 * time.Millisecond},
		{"jitter", 4, 400 * time.Millisecond},
	} {
		if got := retryDelay(tc.strategy, tc.attempt, half); got != tc.want {
			t.Errorf("retryDelay(%s, %d) = %s, want %s", tc.strategy, tc.attempt, got, tc.want)
		}
	}
}

// fakeFrontend answers StartWork with startStatus and GetOperation with
// opStatus/opBody, counting the StartWork calls.
func fakeFrontend(t *testing.T, startStatus, opStatus int, opBody string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var starts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("room") != "r1" {
			http.Error(w, "missing or invalid room", http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/api/start":
			starts.Add(1)
			if startStatus != http.StatusOK {
				http.Error(w, "no", startStatus)
				return
			}
			fmt.Fprint(w, `{"operation_id":"1-abc"}`)
		case "/api/op":
			w.WriteHeader(opStatus)
			fmt.Fprint(w, opBody)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &starts
}

func runOne(srv *httptest.Server, cfg ClientConfig) Result {
	rec := newRecorder(time.Now())
	c := &client{base: srv.URL, room: "r1", http: srv.Client(), cfg: cfg, rec: rec}
	c.runLogicalRequest(context.Background())
	return summarize(Scenario{Cooldown: time.Minute}, rec.timeline())
}

func TestClientPolicy(t *testing.T) {
	retry := ClientConfig{Deadline: 300 * time.Millisecond, Retry: "immediate", MaxRetries: 2}

	t.Run("success on first poll", func(t *testing.T) {
		srv, starts := fakeFrontend(t, 200, 200, `{"status":"done"}`)
		r := runOne(srv, retry)
		if r.Succeeded != 1 || starts.Load() != 1 {
			t.Errorf("succeeded=%d starts=%d, want 1 and 1", r.Succeeded, starts.Load())
		}
	})
	t.Run("timeout is retried up to max retries", func(t *testing.T) {
		srv, starts := fakeFrontend(t, 200, 200, `{"status":"pending"}`)
		r := runOne(srv, retry)
		if r.Succeeded != 0 || r.Attempts != 3 || starts.Load() != 3 {
			t.Errorf("succeeded=%d attempts=%d starts=%d, want 0, 3, 3", r.Succeeded, r.Attempts, starts.Load())
		}
	})
	t.Run("no retries with strategy none", func(t *testing.T) {
		srv, starts := fakeFrontend(t, 200, 200, `{"status":"pending"}`)
		runOne(srv, ClientConfig{Deadline: 300 * time.Millisecond, Retry: "none", MaxRetries: 5})
		if starts.Load() != 1 {
			t.Errorf("starts=%d, want 1", starts.Load())
		}
	})
	t.Run("shed is honored, not retried", func(t *testing.T) {
		srv, starts := fakeFrontend(t, 429, 200, "")
		r := runOne(srv, retry)
		if r.Shed != 1 || starts.Load() != 1 {
			t.Errorf("shed=%d starts=%d, want 1 and 1", r.Shed, starts.Load())
		}
	})
	t.Run("cleared operation (404) fails fast and is retried", func(t *testing.T) {
		srv, starts := fakeFrontend(t, 200, 404, "")
		begin := time.Now()
		runOne(srv, ClientConfig{Deadline: 5 * time.Second, Retry: "immediate", MaxRetries: 2})
		if starts.Load() != 3 {
			t.Errorf("starts=%d, want 3", starts.Load())
		}
		if d := time.Since(begin); d > 2*time.Second {
			t.Errorf("took %s; a 404 should not wait out the 5 s deadline", d)
		}
	})
}

// timeline builds one second per entry from (logical, cohortOK, queue) rows.
func timeline(rows ...[3]int) []second {
	var tl []second
	for i, r := range rows {
		tl = append(tl, second{T: i, Logical: r[0], Attempts: r[0], Completed: r[1], CohortOK: r[1],
			Failed: r[0] - r[1], QueueMax: int64(r[2]), QueueLast: int64(r[2])})
	}
	return tl
}

func TestSummarize(t *testing.T) {
	sc := Scenario{Client: ClientConfig{QPS: 10, SpikeQPS: 50}, Warmup: time.Second, Spike: 2 * time.Second, Cooldown: 5 * time.Second}

	// Healthy, 2 s spike that half-fails, 1 s hangover, then healthy again.
	r := summarize(sc, timeline(
		[3]int{10, 10, 0},
		[3]int{50, 25, 40}, [3]int{50, 25, 90},
		[3]int{10, 2, 60}, [3]int{10, 10, 5}, [3]int{10, 10, 0}, [3]int{10, 10, 0}, [3]int{10, 10, 0},
	))
	if r.Logical != 160 || r.Succeeded != 102 || r.PeakQueue != 90 || r.SpikeGoodput != 25 {
		t.Errorf("got logical=%d succeeded=%d peak=%d spikeGoodput=%v", r.Logical, r.Succeeded, r.PeakQueue, r.SpikeGoodput)
	}
	if r.RecoverySec != 1 {
		t.Errorf("RecoverySec = %d, want 1", r.RecoverySec)
	}

	// Never healthy after the spike.
	r = summarize(sc, timeline(
		[3]int{10, 10, 0},
		[3]int{50, 5, 40}, [3]int{50, 0, 90},
		[3]int{10, 0, 100}, [3]int{10, 0, 110}, [3]int{10, 0, 120}, [3]int{10, 0, 130}, [3]int{10, 0, 140},
	))
	if !r.collapsed() || r.EndQueue != 140 || r.QueueSlope != 10 {
		t.Errorf("collapsed=%v endQueue=%d slope=%v, want a collapse with 140 queued, growing 10/s", r.collapsed(), r.EndQueue, r.QueueSlope)
	}
}

func TestCheck(t *testing.T) {
	res := func(name string, goodput, amp float64, peak int64, recovery int) Result {
		sc := Scenario{Name: name, Client: ClientConfig{QPS: 60}, Server: ServerConfig{InflightQuota: 70}}
		r := Result{Scenario: sc, GoodputPct: goodput, Amplification: amp, PeakQueue: peak, RecoverySec: recovery, QueueAfter5s: 1000}
		if amp > 1 {
			r.QueueSlope = 280 // retries keep the backlog growing
		} else {
			r.QueueSlope = -20
		}
		return r
	}
	good := []Result{
		res("baseline", 100, 1, 0, 0),
		res("fifo", 10, 1, 2000, -1),
		res("lifo", 50, 1, 2000, 0),
		res("margin", 51, 1, 270, 0),
		res("retry-storm", 10, 5.5, 19000, -1),
		res("clear-queue", 12, 5.5, 16000, -1),
		res("shed", 51, 1, 66, 4),
	}
	if fails := check(good); len(fails) != 0 {
		t.Errorf("the README's shape should pass, got: %v", fails)
	}

	bad := append([]Result(nil), good...)
	bad[2] = res("lifo", 12, 1, 2000, 0)         // LIFO no better than FIFO
	bad[4] = res("retry-storm", 10, 5.5, 900, 3) // storm that recovers on its own
	fails := strings.Join(check(bad), "\n")
	for _, want := range []string{"lifo goodput", "retry-storm: 3s after the spike"} {
		if !strings.Contains(fails, want) {
			t.Errorf("expected a failure mentioning %q, got:\n%s", want, fails)
		}
	}
}

func TestScenarioNamesAreValidRoomIDs(t *testing.T) {
	for _, s := range scenarios() {
		for _, c := range s.Name {
			if !(c == '-' || c == '_' || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
				t.Errorf("scenario %q: %q is not allowed in a room id", s.Name, c)
			}
		}
	}
}
