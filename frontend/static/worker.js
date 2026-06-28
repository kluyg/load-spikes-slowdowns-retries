// The browser-based client tier. Each Web Worker is a virtual client that
// issues real RPCs against the frontend: StartWork, then poll GetOperation
// until the operation completes (the Long-Running-Operation pattern).
//
// Phase 0: a single open-loop request generator at a fixed QPS, no retries.
// Retry strategies arrive in a later phase.

let timer = null;
let qps = 20;

const POLL_INTERVAL_MS = 200;
const OP_TIMEOUT_MS = 30000; // safety net so a stuck op doesn't leak forever

self.onmessage = (e) => {
  const msg = e.data;
  switch (msg.type) {
    case "config":
      qps = msg.qps;
      break;
    case "start":
      startLoad();
      break;
    case "stop":
      stopLoad();
      break;
  }
};

function startLoad() {
  stopLoad();
  const intervalMs = 1000 / qps;
  timer = setInterval(fireRequest, intervalMs);
}

function stopLoad() {
  if (timer) {
    clearInterval(timer);
    timer = null;
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
    if (ok) {
      self.postMessage({ type: "completed", latencyMs: performance.now() - t0 });
    } else {
      self.postMessage({ type: "failed" });
    }
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
