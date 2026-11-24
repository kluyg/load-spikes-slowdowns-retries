package main

import (
	"testing"
	"time"
)

func TestSampleLatencyUniform(t *testing.T) {
	p := &roomPool{}
	cfg := Config{LatencyMode: "uniform", LatencyMs: 50}
	if got := sampleLatency(cfg, p); got != 50*time.Millisecond {
		t.Errorf("uniform latency = %s, want 50ms", got)
	}
	p.latencyUntilMs.Store(time.Now().Add(time.Minute).UnixMilli())
	if got := sampleLatency(cfg, p); got != 200*time.Millisecond {
		t.Errorf("latency during the chaos spike = %s, want 4 x 50ms", got)
	}
}

// The long tail must keep the mean (so capacity = workers / mean is unchanged)
// while actually having a tail.
func TestSampleLatencyLongTailPreservesMean(t *testing.T) {
	p := &roomPool{}
	cfg := Config{LatencyMode: "longtail", LatencyMs: 50}
	const n = 200000
	var sum, worst time.Duration
	for i := 0; i < n; i++ {
		d := sampleLatency(cfg, p)
		sum += d
		if d > worst {
			worst = d
		}
	}
	// Each draw is truncated to whole milliseconds, so the mean sits about
	// half a millisecond under 50.
	if mean := sum / n; mean < 47*time.Millisecond || mean > 52*time.Millisecond {
		t.Errorf("long-tail mean = %s, want about 50ms", mean)
	}
	if worst < 250*time.Millisecond {
		t.Errorf("long-tail max over %d draws = %s, want a real tail (> 5x the mean)", n, worst)
	}
}
