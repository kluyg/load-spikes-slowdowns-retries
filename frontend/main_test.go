package main

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketCapsBurstAtOneSecond(t *testing.T) {
	var tb tokenBucket
	tb.last = time.Now().Add(-10 * time.Second) // long idle: tokens must cap, not pile up
	allowed := 0
	for i := 0; i < 100; i++ {
		if tb.allow(20) {
			allowed++
		}
	}
	if allowed < 20 || allowed > 21 {
		t.Errorf("allowed %d of an instant burst at rate 20, want about 20", allowed)
	}
}

func TestOpTimestamp(t *testing.T) {
	for id, want := range map[string]int64{
		"1795539152422-abcdef": 1795539152422,
		"abcdef":               0,
		"-abcdef":              0,
		"x-abcdef":             0,
	} {
		if got := opTimestamp(id); got != want {
			t.Errorf("opTimestamp(%q) = %d, want %d", id, got, want)
		}
	}
}

func TestRoomFromChannel(t *testing.T) {
	for ch, want := range map[string]string{
		"room:abc:completions":   "abc",
		"room:a-b_c:completions": "a-b_c",
		"completions":            "",
		"room:abc:config":        "",
	} {
		if got := roomFromChannel(ch); got != want {
			t.Errorf("roomFromChannel(%q) = %q, want %q", ch, got, want)
		}
	}
}

func TestRoomParamRejectsBadIDs(t *testing.T) {
	s := &server{rooms: map[string]*roomState{}}
	for query, wantOK := range map[string]bool{
		"?room=abc-DEF_123": true,
		"":                  false,
		"?room=":            false,
		"?room=a:b":         false,
		"?room=a%20b":       false,
	} {
		w := httptest.NewRecorder()
		_, ok := s.roomParam(w, httptest.NewRequest("GET", "/api/metrics"+query, nil))
		if ok != wantOK {
			t.Errorf("roomParam(%q) ok = %v, want %v", query, ok, wantOK)
		}
		if !ok && w.Code != 400 {
			t.Errorf("roomParam(%q) wrote status %d, want 400", query, w.Code)
		}
	}
}

// Without Redis these cover only the paths that never reach it: a room we
// already track, and the cap switched off. The cap itself is exercised against
// the real stack.
func TestAdmitKnownRoomIgnoresCap(t *testing.T) {
	s := &server{rooms: map[string]*roomState{}, maxRooms: 1}
	s.roomState("mine")
	if !s.admit(context.Background(), "mine") {
		t.Error("a room that already exists was refused")
	}
}

func TestAdmitUncapped(t *testing.T) {
	s := &server{rooms: map[string]*roomState{}, maxRooms: 0}
	for _, room := range []string{"a", "b", "c"} {
		if !s.admit(context.Background(), room) {
			t.Errorf("room %q refused with the cap disabled", room)
		}
	}
}

func TestGetenvInt(t *testing.T) {
	t.Setenv("MAX_ROOMS_TEST", "7")
	if got := getenvInt("MAX_ROOMS_TEST", 20); got != 7 {
		t.Errorf("getenvInt = %d, want 7", got)
	}
	t.Setenv("MAX_ROOMS_TEST", "lots")
	if got := getenvInt("MAX_ROOMS_TEST", 20); got != 20 {
		t.Errorf("getenvInt with junk = %d, want the default 20", got)
	}
}

func TestConfigUpdateWritesOnlyNamedKnobs(t *testing.T) {
	set, err := configUpdate([]byte(`{"queue_mode":"LIFO","max_qps":0,"not_a_knob":1}`))
	if err != nil {
		t.Fatal(err)
	}
	// max_qps is named with its zero value and must still be written; the seven
	// knobs the body leaves out, and the unknown key, must not be.
	if len(set) != 2 || set["queue_mode"] != "LIFO" || set["max_qps"] != int64(0) {
		t.Errorf("configUpdate = %v, want only queue_mode=LIFO and max_qps=0", set)
	}

	full, err := configUpdate([]byte(`{"queue_mode":"FIFO","queue_max":0,"deadline_mode":"none","margin_ms":70,"latency_mode":"uniform","latency_ms":50,"shed_mode":"none","max_qps":0,"inflight_quota":0}`))
	if err != nil || len(full) != 9 {
		t.Errorf("a full config gave %d fields (err %v), want 9", len(full), err)
	}

	for _, bad := range []string{`{"max_qps":"lots"}`, `[1,2]`, `nope`} {
		if _, err := configUpdate([]byte(bad)); err == nil {
			t.Errorf("configUpdate(%s) accepted a malformed body", bad)
		}
	}
}

func TestTouchDueOncePerWindow(t *testing.T) {
	rs := &roomState{}
	const t0 = int64(1_795_000_000_000)
	if !rs.touchDue(t0) {
		t.Fatal("the first touch of a room was not written")
	}
	for _, later := range []int64{0, 1, 500, 999} {
		if rs.touchDue(t0 + later) {
			t.Errorf("a touch %d ms after the last write was written again", later)
		}
	}
	if !rs.touchDue(t0 + 1000) {
		t.Error("a touch a full window after the last write was skipped")
	}

	// Many requests landing in the same instant must produce a single write.
	var wg sync.WaitGroup
	var wrote atomic.Int32
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rs.touchDue(t0 + 5000) {
				wrote.Add(1)
			}
		}()
	}
	wg.Wait()
	if wrote.Load() != 1 {
		t.Errorf("%d of 64 concurrent touches were written, want 1", wrote.Load())
	}
}
