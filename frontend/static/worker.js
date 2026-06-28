// The browser-based client tier. Each Web Worker is a virtual client that
// issues real RPCs against the frontend: StartWork, then poll GetOperation
// until the operation completes (the Long-Running-Operation pattern).
//
// Phase 1: open-loop request generator at a fixed QPS, plus a short-lived "4x
// load" spike. No retries yet — those arrive in a later phase.

let baseQps = 20;
let running = false;
let timer = null; // request-firing interval
let spikeTimer = null; // ends the load spike

const POLL_INTERVAL_MS = 200;
const OP_TIMEOUT_MS = 30000; // safety net so a stuck op doesn't leak forever

self.onmessage = (e) => {
  const msg = e.data;
  switch (msg.type) {
    case "config":
      baseQps = msg.qps;
      // Don't disturb an active spike; it reverts to the new base when it ends.
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
  const t0 = performance.now();
  self.postMessage({ type: "started" });
  try {
    const res = await fetch("/api/start", { method: "POST" });
    if (!res.ok) {
      self.postMessage({ type: "failed" });
      return;
    }
    const { operation_id } = await res.json();
    const ok = await pollUntilDone(operation_id, t0);
    self.postMessage({ type: ok ? "completed" : "failed", latencyMs: performance.now() - t0 });
  } catch (err) {
    self.postMessage({ type: "failed" });
  }
}

async function pollUntilDone(id, t0) {
  while (performance.now() - t0 < OP_TIMEOUT_MS) {
    await sleep(POLL_INTERVAL_MS);
    try {
      const res = await fetch("/api/op?id=" + encodeURIComponent(id));
      if (!res.ok) continue;
      const { status } = await res.json();
      if (status === "done") return true;
    } catch (err) {
      // transient; keep polling until the timeout
    }
  }
  return false;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
