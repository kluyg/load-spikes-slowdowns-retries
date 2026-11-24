package main

import "time"

// ServerConfig mirrors the frontend's Config struct (POST /api/config).
type ServerConfig struct {
	QueueMode     string `json:"queue_mode"`
	QueueMax      int64  `json:"queue_max"`
	DeadlineMode  string `json:"deadline_mode"`
	MarginMs      int64  `json:"margin_ms"`
	LatencyMode   string `json:"latency_mode"`
	LatencyMs     int64  `json:"latency_ms"`
	ShedMode      string `json:"shed_mode"`
	MaxQPS        int64  `json:"max_qps"`
	InflightQuota int64  `json:"inflight_quota"`
}

// ClientConfig mirrors the knobs of the browser client (static/worker.js).
type ClientConfig struct {
	QPS        float64       `json:"qps"`       // logical requests/s outside the spike
	SpikeQPS   float64       `json:"spike_qps"` // logical requests/s during the spike
	Deadline   time.Duration `json:"deadline_ns"`
	Retry      string        `json:"retry"` // none | immediate | backoff | jitter
	MaxRetries int           `json:"max_retries"`
}

// Scenario is one scripted run: warmup at QPS, a spike at SpikeQPS, then a
// cooldown back at QPS. ClearAfter > 0 fires the operator "Clear queue" lever
// that long after the spike ends.
type Scenario struct {
	Name       string        `json:"name"`
	Desc       string        `json:"desc"`
	Client     ClientConfig  `json:"client"`
	Server     ServerConfig  `json:"server"`
	Warmup     time.Duration `json:"warmup_ns"`
	Spike      time.Duration `json:"spike_ns"`
	Cooldown   time.Duration `json:"cooldown_ns"`
	ClearAfter time.Duration `json:"clear_after_ns"`
}

func (s Scenario) total() time.Duration { return s.Warmup + s.Spike + s.Cooldown }

// due returns how many logical requests should have been started by elapsed
// time t: the integral of the piecewise-constant arrival rate.
func (s Scenario) due(t time.Duration) int {
	if t > s.total() {
		t = s.total()
	}
	seg := func(from, to time.Duration, qps float64) float64 {
		if t <= from {
			return 0
		}
		end := to
		if t < end {
			end = t
		}
		return qps * (end - from).Seconds()
	}
	n := seg(0, s.Warmup, s.Client.QPS) +
		seg(s.Warmup, s.Warmup+s.Spike, s.Client.SpikeQPS) +
		seg(s.Warmup+s.Spike, s.total(), s.Client.QPS)
	return int(n + 1e-9)
}

// Shape of the spike used for every README table. GUESS: the README says "a
// 10 s spike to 5x capacity" (which would be 400/s), but its own numbers --
// 3,775 attempts in the no-retry run, and a peak queue of 263 under a 1 s
// deadline with deadline+margin -- only add up for roughly 280 arrivals/s on
// top of the 60/s preset baseline. See README "Reproducing the numbers".
const (
	baseQPS  = 60
	spikeQPS = 280
)

func defaultServer() ServerConfig {
	return ServerConfig{
		QueueMode: "FIFO", DeadlineMode: "none", MarginMs: 70,
		LatencyMode: "uniform", LatencyMs: 50, ShedMode: "none",
	}
}

func spikeScenario(name, desc string, mut func(*Scenario)) Scenario {
	s := Scenario{
		Name: name, Desc: desc,
		Client: ClientConfig{QPS: baseQPS, SpikeQPS: spikeQPS, Deadline: time.Second, Retry: "none"},
		Server: defaultServer(),
		Warmup: 5 * time.Second, Spike: 10 * time.Second, Cooldown: 15 * time.Second,
	}
	if mut != nil {
		mut(&s)
	}
	return s
}

func immediateRetry(s *Scenario) {
	s.Client.Retry = "immediate"
	s.Client.MaxRetries = 5
}

// scenarios returns the built-in runs, in README order.
func scenarios() []Scenario {
	return []Scenario{
		{
			Name: "baseline", Desc: "Healthy baseline: 60/s (~75% of capacity), no spike",
			Client:   ClientConfig{QPS: baseQPS, SpikeQPS: baseQPS, Deadline: time.Second, Retry: "none"},
			Server:   defaultServer(),
			Cooldown: 10 * time.Second,
		},
		spikeScenario("fifo", "Spike, no retries, FIFO, no deadline drop", nil),
		spikeScenario("lifo", "Spike, no retries, LIFO, no deadline drop", func(s *Scenario) {
			s.Server.QueueMode = "LIFO"
		}),
		spikeScenario("margin", "Spike, no retries, FIFO, deadline + margin", func(s *Scenario) {
			s.Server.DeadlineMode = "margin"
		}),
		spikeScenario("retry-storm", "Spike, immediate retry x5, FIFO, no shedding", immediateRetry),
		spikeScenario("clear-queue", "Retry storm, then operator clears the queue 5 s after the spike", func(s *Scenario) {
			immediateRetry(s)
			s.ClearAfter = 5 * time.Second
		}),
		spikeScenario("shed", "Spike, immediate retry x5, in-flight quota 70", func(s *Scenario) {
			immediateRetry(s)
			s.Server.ShedMode = "inflight"
			s.Server.InflightQuota = 70
		}),
	}
}
