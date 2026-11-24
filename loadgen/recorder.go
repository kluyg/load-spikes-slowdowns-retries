package main

import (
	"sync"
	"time"
)

// second is everything observed during one wall-clock second of a run.
type second struct {
	T         int   `json:"t"`
	Logical   int   `json:"logical"`   // logical requests started
	Attempts  int   `json:"attempts"`  // StartWork calls (offered load)
	Shed      int   `json:"shed"`      // StartWork calls answered 429
	Completed int   `json:"completed"` // logical requests that succeeded (goodput)
	Failed    int   `json:"failed"`    // logical requests that gave up
	CohortOK  int   `json:"cohort_ok"` // of the requests STARTED this second, how many succeeded
	QueueMax  int64 `json:"queue_max"` // server queue_len, max over the second
	QueueLast int64 `json:"queue_last"`
	InFlight  int64 `json:"in_flight"`
	sampled   bool
}

// recorder buckets client-side events and server samples by second.
type recorder struct {
	mu    sync.Mutex
	start time.Time
	secs  []second
}

func newRecorder(start time.Time) *recorder { return &recorder{start: start} }

// at returns the bucket for time t, growing the timeline as needed. Caller
// holds mu.
func (r *recorder) at(t time.Time) *second {
	i := int(t.Sub(r.start) / time.Second)
	if i < 0 {
		i = 0
	}
	for len(r.secs) <= i {
		r.secs = append(r.secs, second{T: len(r.secs)})
	}
	return &r.secs[i]
}

func (r *recorder) logicalStarted() time.Time {
	now := time.Now()
	r.mu.Lock()
	r.at(now).Logical++
	r.mu.Unlock()
	return now
}

func (r *recorder) logicalDone(started time.Time, success bool) {
	now := time.Now()
	r.mu.Lock()
	if success {
		r.at(now).Completed++
		r.at(started).CohortOK++
	} else {
		r.at(now).Failed++
	}
	r.mu.Unlock()
}

func (r *recorder) attempt() {
	now := time.Now()
	r.mu.Lock()
	r.at(now).Attempts++
	r.mu.Unlock()
}

func (r *recorder) shed() {
	now := time.Now()
	r.mu.Lock()
	r.at(now).Shed++
	r.mu.Unlock()
}

func (r *recorder) server(queueLen, inFlight int64) {
	now := time.Now()
	r.mu.Lock()
	s := r.at(now)
	if queueLen > s.QueueMax {
		s.QueueMax = queueLen
	}
	s.QueueLast, s.InFlight, s.sampled = queueLen, inFlight, true
	r.mu.Unlock()
}

func (r *recorder) timeline() []second {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]second(nil), r.secs...)
}
