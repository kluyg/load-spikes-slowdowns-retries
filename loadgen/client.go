package main

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// These mirror frontend/static/worker.js; keep them in sync.
const (
	pollInterval = 250 * time.Millisecond
	backoffBase  = 100 * time.Millisecond
)

type outcome int

const (
	ok outcome = iota
	timeout
	shed
)

// client is the headless twin of the browser Web Worker: the same StartWork +
// poll loop and the same retry policy, driven from Go instead of a page.
type client struct {
	base string
	room string // every RPC is scoped to one isolated room
	http *http.Client
	cfg  ClientConfig
	rec  *recorder
}

func newHTTPClient() *http.Client {
	return &http.Client{
		// The browser's fetch has no timeout. ASSUMPTION: 10 s is far enough
		// past every deadline used here to behave the same, while still
		// letting a run terminate if the frontend hangs.
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        4096,
			MaxIdleConnsPerHost: 4096,
			IdleConnTimeout:     30 * time.Second,
		},
	}
}

// runLogicalRequest drives one user request through attempts under the retry
// policy, exactly like runLogicalRequest in worker.js.
func (c *client) runLogicalRequest(ctx context.Context) {
	started := c.rec.logicalStarted()
	attempt := 0
	for {
		res := c.oneAttempt(ctx)
		if ctx.Err() != nil {
			return // run was cancelled; this request is not counted either way
		}
		if res == ok {
			c.rec.logicalDone(started, true)
			return
		}
		// A shed (429) is honored as backpressure: stop, don't retry.
		if res == shed || c.cfg.Retry == "none" || attempt >= c.cfg.MaxRetries {
			c.rec.logicalDone(started, false)
			return
		}
		attempt++
		if !sleepCtx(ctx, retryDelay(c.cfg.Retry, attempt, rand.Float64)) {
			return
		}
	}
}

func retryDelay(strategy string, attempt int, rnd func() float64) time.Duration {
	exp := backoffBase << (attempt - 1)
	switch strategy {
	case "backoff":
		return exp
	case "jitter":
		return time.Duration(rnd() * float64(exp)) // full jitter
	}
	return 0 // immediate
}

// oneAttempt makes a single StartWork call and polls until its deadline.
func (c *client) oneAttempt(ctx context.Context) outcome {
	c.rec.attempt()
	deadline := time.Now().Add(c.cfg.Deadline)
	u := c.base + "/api/start?deadline_ms=" + strconv.FormatInt(deadline.UnixMilli(), 10) + "&room=" + c.room
	status, body := c.do(ctx, http.MethodPost, u)
	if status == http.StatusTooManyRequests {
		c.rec.shed()
		return shed
	}
	if status != http.StatusOK {
		return timeout
	}
	var start struct {
		OperationID string `json:"operation_id"`
	}
	if json.Unmarshal(body, &start) != nil || start.OperationID == "" {
		return timeout
	}
	if c.pollUntilDone(ctx, start.OperationID, deadline) {
		return ok
	}
	return timeout
}

func (c *client) pollUntilDone(ctx context.Context, id string, deadline time.Time) bool {
	u := c.base + "/api/op?id=" + url.QueryEscape(id) + "&room=" + c.room
	for time.Now().Before(deadline) {
		if !sleepCtx(ctx, pollInterval) {
			return false
		}
		status, body := c.do(ctx, http.MethodGet, u)
		if status == http.StatusNotFound {
			return false // op was cleared; give up now and retry fresh
		}
		if status != http.StatusOK {
			continue // transient (including a shed poll); keep polling
		}
		var op struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(body, &op) == nil && op.Status == "done" {
			return true
		}
	}
	return false
}

// do returns the status code and body, or status 0 on a transport error.
func (c *client) do(ctx context.Context, method, u string) (int, []byte) {
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return 0, nil
	}
	res, err := c.http.Do(req)
	if err != nil {
		return 0, nil
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	return res.StatusCode, body
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
