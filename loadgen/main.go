// Command loadgen is a headless twin of the browser client tier. It drives the
// running system (frontend + backend + Redis) through scripted scenarios --
// warmup, load spike, cooldown -- with the same StartWork/poll loop and retry
// policy as static/worker.js, and reports goodput, offered amplification and
// queue depth. It is how the tables in the README are produced:
//
//	docker compose up --build -d
//	cd loadgen && go run . -format markdown
//
// With -check it also asserts the shapes the README claims (orderings, not
// absolute numbers) and exits non-zero if one does not hold; CI runs that.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	base := flag.String("url", "http://localhost:8080", "frontend base URL")
	names := flag.String("scenario", "all", "comma-separated scenario names, or \"all\"")
	list := flag.Bool("list", false, "list scenarios and exit")
	format := flag.String("format", "table", "output: table | markdown | json")
	doCheck := flag.Bool("check", false, "assert the README's claims hold; exit 1 if not")
	spike := flag.Float64("spike-qps", 0, "override the spike arrival rate (logical requests/s)")
	qps := flag.Float64("qps", 0, "override the baseline arrival rate (logical requests/s)")
	cooldown := flag.Duration("cooldown", 0, "override the cooldown length")
	roomPrefix := flag.String("room", "loadgen", "room id prefix; each scenario runs in its own fresh room")
	wait := flag.Duration("wait", 30*time.Second, "how long to wait for the frontend to come up")
	flag.Parse()

	all := scenarios()
	if *list {
		for _, s := range all {
			fmt.Printf("%-12s %s\n", s.Name, s.Desc)
		}
		return
	}
	var run []Scenario
	for _, s := range all {
		if *names == "all" || contains(strings.Split(*names, ","), s.Name) {
			if *qps > 0 {
				if s.Client.SpikeQPS == s.Client.QPS {
					s.Client.SpikeQPS = *qps
				}
				s.Client.QPS = *qps
			}
			if *spike > 0 && s.Spike > 0 {
				s.Client.SpikeQPS = *spike
			}
			if *cooldown > 0 {
				s.Cooldown = *cooldown
			}
			run = append(run, s)
		}
	}
	if len(run) == 0 {
		fatalf("no scenario matches %q (try -list)", *names)
	}

	d := &driver{base: strings.TrimRight(*base, "/"), http: newHTTPClient()}
	d.room = *roomPrefix + "-ready"
	if err := d.waitReady(*wait); err != nil {
		fatalf("%v", err)
	}
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])

	var results []Result
	for _, sc := range run {
		fmt.Fprintf(os.Stderr, "running %-12s (%s) ...\n", sc.Name, sc.total())
		// A fresh room per scenario: its own queue, knobs, metrics and worker
		// pool, so runs cannot contaminate each other or anyone using the UI.
		d.room = fmt.Sprintf("%s-%s-%s", *roomPrefix, sc.Name, hex.EncodeToString(suffix[:]))
		res, err := d.run(sc)
		if err != nil {
			fatalf("%s: %v", sc.Name, err)
		}
		results = append(results, res)
	}
	switch *format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
	case "markdown":
		writeMarkdown(os.Stdout, results)
	default:
		writeTable(os.Stdout, results)
	}

	if *doCheck {
		fails := check(results)
		for _, f := range fails {
			fmt.Fprintln(os.Stderr, "FAIL:", f)
		}
		if len(fails) > 0 {
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "all checks passed")
	}
}

type driver struct {
	base string
	room string
	http *http.Client
}

type metrics struct {
	QueueLen int64 `json:"queue_len"`
	InFlight int64 `json:"in_flight"`
}

func (d *driver) metrics() (metrics, error) {
	var m metrics
	res, err := d.http.Get(d.base + "/api/metrics?room=" + d.room)
	if err != nil {
		return m, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return m, fmt.Errorf("GET /api/metrics: %s", res.Status)
	}
	return m, json.NewDecoder(res.Body).Decode(&m)
}

func (d *driver) post(path string, body any) error {
	var r *strings.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = strings.NewReader(string(b))
	} else {
		r = strings.NewReader("")
	}
	res, err := d.http.Post(d.base+path+"?room="+d.room, "application/json", r)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s: %s", path, res.Status)
	}
	return nil
}

func (d *driver) waitReady(max time.Duration) error {
	deadline := time.Now().Add(max)
	for {
		_, err := d.metrics()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("frontend at %s not reachable: %w", d.base, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// reset applies the knobs, drops any backlog, and waits for the room to go
// idle so each scenario starts from a clean slate. In a new room this is also
// what gives the backend time to notice the room and start its worker pool.
func (d *driver) reset(cfg ServerConfig) error {
	if err := d.post("/api/config", cfg); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for quiet := 0; quiet < 4; {
		if err := d.post("/api/admin/clear-queue", nil); err != nil {
			return err
		}
		// Also covers the 250 ms the frontend and backend take to pick up config.
		time.Sleep(300 * time.Millisecond)
		m, err := d.metrics()
		if err != nil {
			return err
		}
		if m.QueueLen == 0 && m.InFlight == 0 {
			quiet++
		} else {
			quiet = 0
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("system did not go idle (queue=%d in-flight=%d)", m.QueueLen, m.InFlight)
		}
	}
	return nil
}

func (d *driver) run(sc Scenario) (Result, error) {
	if err := d.reset(sc.Server); err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	rec := newRecorder(start)
	c := &client{base: d.base, room: d.room, http: d.http, cfg: sc.Client, rec: rec}

	// Server-side sampler: queue depth and in-flight, at the server's own rate.
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if m, err := d.metrics(); err == nil {
					rec.server(m.QueueLen, m.InFlight)
				}
			}
		}
	}()

	if sc.ClearAfter > 0 {
		at := sc.Warmup + sc.Spike + sc.ClearAfter
		go func() {
			if sleepCtx(ctx, time.Until(start.Add(at))) {
				_ = d.post("/api/admin/clear-queue", nil)
			}
		}()
	}

	// Open-loop arrivals: start however many logical requests are due, whether
	// or not earlier ones have finished -- a slow system does not slow its
	// clients down, which is the whole problem.
	var wg sync.WaitGroup
	tick := time.NewTicker(2 * time.Millisecond)
	started := 0
	for elapsed := time.Duration(0); elapsed < sc.total(); elapsed = time.Since(start) {
		for due := sc.due(elapsed); started < due; started++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.runLogicalRequest(ctx)
			}()
		}
		<-tick.C
	}
	tick.Stop()

	// Let requests already in the air finish (or exhaust their retries), then
	// cut off whatever is left.
	drain := time.Duration(sc.Client.MaxRetries+1)*(sc.Client.Deadline+pollInterval) + 5*time.Second
	idle := make(chan struct{})
	go func() { wg.Wait(); close(idle) }()
	select {
	case <-idle:
	case <-time.After(drain):
	}
	cancel()
	<-samplerDone
	wg.Wait()

	return summarize(sc, rec.timeline()), nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if strings.TrimSpace(v) == x {
			return true
		}
	}
	return false
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadgen: "+format+"\n", args...)
	os.Exit(2)
}
