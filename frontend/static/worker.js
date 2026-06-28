// The browser-based client tier. Each Web Worker is a virtual client that
// issues real RPCs against the frontend: StartWork, then poll GetOperation
// until the operation completes or the client's deadline passes (the
// Long-Running-Operation pattern).
//
// Phase 2a: each request carries a deadline. If the op isn't done by then, the
// client gives up — that request counts as failed, not goodput. This is what
// makes goodput diverge from throughput under overload. No retries yet.

let baseQps = 60;
let clientDeadlineMs = 1000;
let running = false;
let timer = null; // request-firing interval
let spikeTimer = null; // ends the load spike

const POLL_INTERVAL_MS = 250;

self.onmessage = (e) => {
  const msg = e.data;
  switch (msg.type) {
    case "config":
      if (msg.qps != null) baseQps = msg.qps;
      if (msg.clientDeadlineMs != null) clientDeadlineMs = msg.clientDeadlineMs;
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
  timer = setInterval(fireRequest, 1000 / qps);
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

async function fireRequest() {
  const deadline = Date.now() + clientDeadlineMs;
  self.postMessage({ type: "started" });
  try {
    const res = await fetch("/api/start?deadline_ms=" + deadline, { method: "POST" });
    if (!res.ok) {
      // Rejected at the edge (e.g. queue full) — the client gave up.
      self.postMessage({ type: "failed" });
      return;
    }
    const { operation_id } = await res.json();
    const ok = await pollUntilDone(operation_id, deadline);
    self.postMessage({ type: ok ? "completed" : "failed" });
  } catch (err) {
    self.postMessage({ type: "failed" });
  }
}

async function pollUntilDone(id, deadline) {
  while (Date.now() < deadline) {
    await sleep(POLL_INTERVAL_MS);
    try {
      const res = await fetch("/api/op?id=" + encodeURIComponent(id));
      if (!res.ok) continue;
      const { status } = await res.json();
      if (status === "done") return true;
    } catch (err) {
      // transient; keep polling until the deadline
    }
  }
  return false; // client gave up
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
