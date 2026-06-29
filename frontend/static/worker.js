// The browser-based client tier. Each Web Worker is a virtual client that
// issues real RPCs against the frontend. A *logical request* is one unit of work
// the user wants done; it may take several *attempts* (StartWork + poll) under a
// retry policy. Goodput counts logical requests that eventually succeed; offered
// load counts attempts — and the gap between them is how retries manufacture new
// load during overload (the metastable trap).

let baseQps = 60;
let clientDeadlineMs = 1000;
let retryStrategy = "none"; // none | immediate | backoff | jitter
let maxRetries = 0;
let running = false;
let timer = null; // logical-request-firing interval
let spikeTimer = null; // ends the load spike

const POLL_INTERVAL_MS = 250;
const BACKOFF_BASE_MS = 100;

self.onmessage = (e) => {
  const msg = e.data;
  switch (msg.type) {
    case "config":
      if (msg.qps != null) baseQps = msg.qps;
      if (msg.clientDeadlineMs != null) clientDeadlineMs = msg.clientDeadlineMs;
      if (msg.retryStrategy != null) retryStrategy = msg.retryStrategy;
      if (msg.maxRetries != null) maxRetries = msg.maxRetries;
      if (running && spikeTimer === null) arm(baseQps);
      break;
    case "start":
      running = true;
      arm(baseQps);
      break;
    case "stop":
      running = false;
      clearTimers();
      break;
    case "spike":
      if (!running) break;
      arm(baseQps * msg.factor);
      clearTimeout(spikeTimer);
      spikeTimer = setTimeout(() => {
        spikeTimer = null;
        if (running) arm(baseQps);
      }, msg.durationMs);
      break;
  }
};

function arm(qps) {
  if (timer) clearInterval(timer);
  timer = setInterval(runLogicalRequest, 1000 / qps);
}

function clearTimers() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  if (spikeTimer) {
    clearTimeout(spikeTimer);
    spikeTimer = null;
  }
}

// runLogicalRequest drives one user request through attempts under the retry
// policy. Each retry is a fresh StartWork — that is the new load retries add.
async function runLogicalRequest() {
  self.postMessage({ type: "started" });
  let attempt = 0;
  while (true) {
    if (await oneAttempt()) {
      self.postMessage({ type: "completed" });
      return;
    }
    if (retryStrategy === "none" || attempt >= maxRetries) {
      self.postMessage({ type: "failed" });
      return;
    }
    attempt++;
    await sleep(retryDelay(attempt));
  }
}

function retryDelay(attempt) {
  if (retryStrategy === "immediate") return 0;
  const exp = BACKOFF_BASE_MS * 2 ** (attempt - 1);
  if (retryStrategy === "backoff") return exp;
  if (retryStrategy === "jitter") return Math.random() * exp; // full jitter
  return 0;
}

// oneAttempt makes a single StartWork call and polls until its deadline.
async function oneAttempt() {
  const deadline = Date.now() + clientDeadlineMs;
  try {
    const res = await fetch("/api/start?deadline_ms=" + deadline, { method: "POST" });
    if (!res.ok) return false; // rejected at the edge
    const { operation_id } = await res.json();
    return await pollUntilDone(operation_id, deadline);
  } catch (err) {
    return false;
  }
}

async function pollUntilDone(id, deadline) {
  while (Date.now() < deadline) {
    await sleep(POLL_INTERVAL_MS);
    try {
      const res = await fetch("/api/op?id=" + encodeURIComponent(id));
      if (res.status === 404) return false; // op was cleared; give up now and retry fresh
      if (!res.ok) continue;
      const { status } = await res.json();
      if (status === "done") return true;
    } catch (err) {
      // transient; keep polling until the deadline
    }
  }
  return false; // this attempt timed out
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
