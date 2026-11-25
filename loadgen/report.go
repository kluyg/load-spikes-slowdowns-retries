package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// Result is the summary of one scenario run.
type Result struct {
	Scenario Scenario `json:"scenario"`

	Logical       int      `json:"logical"`        // logical requests started
	Attempts      int      `json:"attempts"`       // StartWork calls
	Succeeded     int      `json:"succeeded"`      // logical requests that succeeded in time
	Shed          int      `json:"shed"`           // StartWork calls answered 429
	Amplification float64  `json:"amplification"`  // attempts / logical
	GoodputPct    float64  `json:"goodput_pct"`    // succeeded / logical
	SpikeGoodput  float64  `json:"spike_goodput"`  // successes/s during the spike
	PeakQueue     int64    `json:"peak_queue"`     // max server queue_len
	EndQueue      int64    `json:"end_queue"`      // queue_len when the cooldown ends
	QueueSlope    float64  `json:"queue_slope"`    // items/s the queue grew (+) or drained (-) over the last 5 s
	TailAttempts  float64  `json:"tail_attempts"`  // attempts/s over the last 5 s of cooldown
	TailGoodput   float64  `json:"tail_goodput"`   // successes/s over the last 5 s of cooldown
	RecoverySec   int      `json:"recovery_sec"`   // s after the spike until requests succeed again; -1 = never
	QueueAtClear  int64    `json:"queue_at_clear"` // clear-queue runs: queue just before the clear
	QueueAfter5s  int64    `json:"queue_after_5s"` // ... and 5 s after it
	Unresolved    int      `json:"unresolved"`     // logical requests still open when the run was cut off
	Timeline      []second `json:"timeline"`
}

const tailWindow = 5 // seconds

// recoveredRatio is the share of a second's requests that must succeed for the
// system to count as healthy again.
const recoveredRatio = 0.9

func summarize(sc Scenario, tl []second) Result {
	r := Result{Scenario: sc, Timeline: tl, RecoverySec: -1}
	spikeFrom := int(sc.Warmup / time.Second)
	spikeTo := int((sc.Warmup + sc.Spike) / time.Second)
	end := int(sc.total() / time.Second)
	if end > len(tl) {
		end = len(tl)
	}
	tailFrom := end - tailWindow
	if tailFrom < spikeTo {
		tailFrom = spikeTo
	}
	clearAt := -1
	if sc.ClearAfter > 0 {
		clearAt = spikeTo + int(sc.ClearAfter/time.Second)
	}

	var spikeDone, tailAttempts, tailDone, resolved int
	var tailStartQueue int64
	for i, s := range tl {
		r.Logical += s.Logical
		r.Attempts += s.Attempts
		r.Succeeded += s.Completed
		r.Shed += s.Shed
		resolved += s.Completed + s.Failed
		if s.QueueMax > r.PeakQueue {
			r.PeakQueue = s.QueueMax
		}
		if i >= spikeFrom && i < spikeTo {
			spikeDone += s.Completed
		}
		if i >= tailFrom && i < end {
			tailAttempts += s.Attempts
			tailDone += s.Completed
		}
		if i == tailFrom-1 {
			tailStartQueue = s.QueueLast
		}
		if i == end-1 {
			r.EndQueue = s.QueueLast
		}
		if i == clearAt-1 {
			r.QueueAtClear = s.QueueMax
		}
		if i == clearAt+tailWindow-1 {
			r.QueueAfter5s = s.QueueLast
		}
	}
	r.Unresolved = r.Logical - resolved
	if r.Logical > 0 {
		r.Amplification = float64(r.Attempts) / float64(r.Logical)
		r.GoodputPct = 100 * float64(r.Succeeded) / float64(r.Logical)
	}
	if spikeTo > spikeFrom {
		r.SpikeGoodput = float64(spikeDone) / float64(spikeTo-spikeFrom)
	}
	if end > tailFrom {
		r.TailAttempts = float64(tailAttempts) / float64(end-tailFrom)
		r.TailGoodput = float64(tailDone) / float64(end-tailFrom)
		r.QueueSlope = float64(r.EndQueue-tailStartQueue) / float64(end-tailFrom)
	}

	// Recovery: the first second after the spike from which every later
	// second's requests succeed. Judged by start time (cohort), so a request
	// counts for the second it was issued in, not the one it finished in.
	healthyFrom := end
	for i := end - 1; i >= spikeTo; i-- {
		s := tl[i]
		if s.Logical > 0 && float64(s.CohortOK) < recoveredRatio*float64(s.Logical) {
			break
		}
		healthyFrom = i
	}
	// Require a few healthy seconds of evidence before calling it recovered.
	if end-healthyFrom >= 3 {
		r.RecoverySec = healthyFrom - spikeTo
	}
	return r
}

func (r Result) recovered() bool { return r.RecoverySec >= 0 }

// collapsed reports a system that is neither healthy nor on its way back: the
// backlog is still growing at the end of the run.
func (r Result) collapsed() bool { return !r.recovered() && r.QueueSlope > 0 }

func (r Result) recoveryText() string {
	switch {
	case r.recovered():
		return fmt.Sprintf("%ds", r.RecoverySec)
	case r.collapsed():
		return "collapsed"
	}
	return "draining"
}

func writeTable(w io.Writer, results []Result) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "scenario\tlogical\tattempts\tamplif.\tgoodput\tspike goodput/s\tpeak queue\tend queue\ttail attempts/s\trecovery\t")
	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.2fx\t%.0f%%\t%.0f\t%d\t%d\t%.0f\t%s\t\n",
			r.Scenario.Name, r.Logical, r.Attempts, r.Amplification, r.GoodputPct,
			r.SpikeGoodput, r.PeakQueue, r.EndQueue, r.TailAttempts, r.recoveryText())
	}
	tw.Flush()
}

// writeMarkdown prints the results in the shape of the README's tables.
func writeMarkdown(w io.Writer, results []Result) {
	by := map[string]Result{}
	for _, r := range results {
		by[r.Scenario.Name] = r
	}
	has := func(names ...string) bool {
		for _, n := range names {
			if _, ok := by[n]; !ok {
				return false
			}
		}
		return true
	}
	if has("fifo", "lifo", "margin") {
		fmt.Fprintln(w, "| Backend policy | Goodput (of offered) | Peak queue |\n|---|---|---|")
		for _, row := range [][2]string{{"fifo", "FIFO, no deadline drop"}, {"lifo", "LIFO, no deadline drop"}, {"margin", "FIFO, deadline + margin"}} {
			r := by[row[0]]
			fmt.Fprintf(w, "| %s | %.0f%% | %d |\n", row[1], r.GoodputPct, r.PeakQueue)
		}
		fmt.Fprintln(w)
	}
	if has("fifo", "retry-storm") {
		fmt.Fprintln(w, "| Client policy | Offered amplification | After the spike ends |\n|---|---|---|")
		for _, row := range [][2]string{{"fifo", "No retry"}, {"retry-storm", "Immediate retry ×5"}} {
			r := by[row[0]]
			fmt.Fprintf(w, "| %s | %.1f× (%s attempts) | %s |\n", row[1], r.Amplification, commas(r.Attempts), afterSpike(r))
		}
		fmt.Fprintln(w)
	}
	if has("clear-queue") {
		r := by["clear-queue"]
		fmt.Fprintf(w, "Clear queue: %d queued just before the clear, %d five seconds after, %s at the end of the run.\n\n",
			r.QueueAtClear, r.QueueAfter5s, afterSpike(r))
	}
	if has("retry-storm", "shed") {
		off, on := by["retry-storm"], by["shed"]
		fmt.Fprintf(w, "| | Shedding off | In-flight quota = %d |\n|---|---|---|\n", on.Scenario.Server.InflightQuota)
		fmt.Fprintf(w, "| Peak queue | %d | %d |\n", off.PeakQueue, on.PeakQueue)
		fmt.Fprintf(w, "| Goodput during spike | %.0f/s | %.0f/s |\n", off.SpikeGoodput, on.SpikeGoodput)
		fmt.Fprintf(w, "| After the spike | %s | %s |\n", afterSpike(off), afterSpike(on))
		fmt.Fprintf(w, "| Offered amplification | %.2f× | %.2f× |\n", off.Amplification, on.Amplification)
		fmt.Fprintf(w, "| Total goodput | %.0f%% | %.0f%% |\n", off.GoodputPct, on.GoodputPct)
		fmt.Fprintln(w)
	}
	if has("retry-storm", "margin-retry") {
		off, on := by["retry-storm"], by["margin-retry"]
		fmt.Fprintln(w, "| | No deadline drop | Deadline + margin |\n|---|---|---|")
		fmt.Fprintf(w, "| Peak queue | %d | %d |\n", off.PeakQueue, on.PeakQueue)
		fmt.Fprintf(w, "| Goodput during spike | %.0f/s | %.0f/s |\n", off.SpikeGoodput, on.SpikeGoodput)
		fmt.Fprintf(w, "| After the spike | %s | %s |\n", afterSpike(off), afterSpike(on))
		fmt.Fprintf(w, "| Offered amplification | %.2f× | %.2f× |\n", off.Amplification, on.Amplification)
		fmt.Fprintf(w, "| Total goodput | %.0f%% | %.0f%% |\n", off.GoodputPct, on.GoodputPct)
		fmt.Fprintln(w)
	}
}

func afterSpike(r Result) string {
	switch {
	case r.recovered():
		return fmt.Sprintf("recovers in ~%ds (queue %d at end)", r.RecoverySec, r.EndQueue)
	case r.collapsed():
		return fmt.Sprintf("never recovers: offered stuck at %.0f/s, queue %d and growing %.0f/s",
			r.TailAttempts, r.EndQueue, r.QueueSlope)
	}
	return fmt.Sprintf("queue drains at %.0f/s (%d left after %s)", -r.QueueSlope, r.EndQueue, r.Scenario.Cooldown)
}

func commas(n int) string {
	s := fmt.Sprint(n)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// check asserts the SHAPES the README claims, not its absolute numbers: this
// is a real system, so the numbers move run to run but the orderings must not.
// Thresholds are deliberately loose. Returns one message per violated claim.
func check(results []Result) []string {
	by := map[string]Result{}
	for _, r := range results {
		by[r.Scenario.Name] = r
	}
	var fails []string
	claim := func(cond bool, format string, args ...any) {
		if !cond {
			fails = append(fails, fmt.Sprintf(format, args...))
		}
	}
	both := func(a, b string) (Result, Result, bool) {
		ra, oka := by[a]
		rb, okb := by[b]
		return ra, rb, oka && okb
	}
	if r, ok := by["baseline"]; ok {
		claim(r.GoodputPct >= 95, "baseline: goodput %.0f%%, want >= 95%%", r.GoodputPct)
	}
	if r, ok := by["fifo"]; ok {
		claim(r.Amplification < 1.01, "fifo: amplification %.2fx without retries, want 1x", r.Amplification)
		claim(!r.collapsed(), "fifo: queue still growing %.0f/s after the spike, want it draining without retries", r.QueueSlope)
	}
	if f, l, ok := both("fifo", "lifo"); ok {
		claim(l.GoodputPct >= f.GoodputPct+15, "lifo goodput %.0f%% should beat fifo %.0f%% by 15+ points", l.GoodputPct, f.GoodputPct)
	}
	if f, m, ok := both("fifo", "margin"); ok {
		claim(m.GoodputPct >= f.GoodputPct+15, "margin goodput %.0f%% should beat fifo %.0f%% by 15+ points", m.GoodputPct, f.GoodputPct)
		claim(m.PeakQueue*2 < f.PeakQueue, "margin peak queue %d should be under half of fifo's %d", m.PeakQueue, f.PeakQueue)
		claim(m.recovered(), "margin: should recover after the spike")
	}
	if r, ok := by["retry-storm"]; ok {
		claim(r.Amplification >= 3, "retry-storm: amplification %.2fx, want >= 3x", r.Amplification)
		claim(r.collapsed(), "retry-storm: %s after the spike, want a lasting collapse", r.recoveryText())
	}
	if r, ok := by["clear-queue"]; ok {
		claim(r.collapsed(), "clear-queue: %s after the spike, want the collapse to survive the clear", r.recoveryText())
		claim(r.QueueAfter5s > int64(r.Scenario.Client.QPS), "clear-queue: queue %d five seconds after the clear, want it re-saturated", r.QueueAfter5s)
	}
	if r, ok := by["shed"]; ok {
		q := r.Scenario.Server.InflightQuota
		claim(r.Amplification <= 1.5, "shed: amplification %.2fx, want about 1x", r.Amplification)
		claim(r.PeakQueue <= 2*q, "shed: peak queue %d, want it pinned near the quota %d", r.PeakQueue, q)
		claim(r.recovered(), "shed: should recover after the spike")
	}
	if s, o, ok := both("shed", "retry-storm"); ok {
		claim(s.GoodputPct >= o.GoodputPct+20, "shed goodput %.0f%% should beat no-shedding %.0f%% by 20+ points", s.GoodputPct, o.GoodputPct)
	}
	if r, ok := by["margin-retry"]; ok {
		claim(r.Amplification >= 2, "margin-retry: amplification %.2fx, want the retry storm to still form (>= 2x)", r.Amplification)
		claim(r.recovered(), "margin-retry: should recover after the spike")
	}
	if m, o, ok := both("margin-retry", "retry-storm"); ok {
		claim(m.GoodputPct >= o.GoodputPct+20, "margin-retry goodput %.0f%% should beat no-deadline-drop %.0f%% by 20+ points", m.GoodputPct, o.GoodputPct)
		claim(m.PeakQueue*2 < o.PeakQueue, "margin-retry peak queue %d should be under half of the retry storm's %d", m.PeakQueue, o.PeakQueue)
	}
	return fails
}
