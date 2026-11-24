package main

import (
	"net/http/httptest"
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
