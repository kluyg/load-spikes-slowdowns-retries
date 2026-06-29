// Main-thread controller: owns the worker (client tier), measures client-side
// goodput, consumes the server's SSE metric stream, drives the live config
// panel, and renders the dashboard.

const SPIKE_MS = 8000; // keep in sync with backend chaosDuration

const worker = new Worker("/worker.js");

// Client-observed goodput (requests that completed before their deadline).
let completedThisSecond = 0;
let running = false;

let server = {
  start_work_qps: 0,
  get_op_qps: 0,
  rpc_failure_ps: 0,
  throughput_ps: 0,
  in_flight: 0,
  queue_len: 0,
};

const goodputHistory = [];
const tputHistory = [];
const queueHistory = [];

worker.onmessage = (e) => {
  if (e.data.type === "completed") completedThisSecond++;
};

// --- Server metric stream (SSE) --------------------------------------------

const es = new EventSource("/api/stream");
es.onmessage = (e) => {
  try {
    server = JSON.parse(e.data);
  } catch (err) {
    /* ignore malformed frame */
  }
};

// --- Elements ---------------------------------------------------------------

const el = (id) => document.getElementById(id);
const toggleBtn = el("toggle");
const qpsInput = el("qps");
const clientDeadlineInput = el("client-deadline");
const retryStrategyInput = el("retry-strategy");
const maxRetriesInput = el("max-retries");
const spikeLoadBtn = el("spike-load");
const spikeLatencyBtn = el("spike-latency");
const clearQueueBtn = el("clear-queue");

// Backend-config controls (mirror the Config struct on the server).
const cfgInputs = {
  shed_mode: el("shed-mode"),
  max_qps: el("max-qps"),
  inflight_quota: el("inflight-quota"),
  queue_mode: el("queue-mode"),
  queue_max: el("queue-max"),
  deadline_mode: el("deadline-mode"),
  margin_ms: el("margin-ms"),
  latency_mode: el("latency-mode"),
  latency_ms: el("latency-ms"),
};

// --- Config sync ------------------------------------------------------------

async function loadConfig() {
  try {
    const c = await (await fetch("/api/config")).json();
    cfgInputs.shed_mode.value = c.shed_mode;
    cfgInputs.max_qps.value = c.max_qps;
    cfgInputs.inflight_quota.value = c.inflight_quota;
    cfgInputs.queue_mode.value = c.queue_mode;
    cfgInputs.queue_max.value = c.queue_max;
    cfgInputs.deadline_mode.value = c.deadline_mode;
    cfgInputs.margin_ms.value = c.margin_ms;
    cfgInputs.latency_mode.value = c.latency_mode;
    cfgInputs.latency_ms.value = c.latency_ms;
  } catch (err) {
    /* keep the HTML defaults */
  }
}

async function postConfig() {
  const body = {
    shed_mode: cfgInputs.shed_mode.value,
    max_qps: Number(cfgInputs.max_qps.value),
    inflight_quota: Number(cfgInputs.inflight_quota.value),
    queue_mode: cfgInputs.queue_mode.value,
    queue_max: Number(cfgInputs.queue_max.value),
    deadline_mode: cfgInputs.deadline_mode.value,
    margin_ms: Number(cfgInputs.margin_ms.value),
    latency_mode: cfgInputs.latency_mode.value,
    latency_ms: Number(cfgInputs.latency_ms.value),
  };
  try {
    await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    /* ignore */
  }
}

Object.values(cfgInputs).forEach((input) => (input.onchange = postConfig));

// --- Controls ---------------------------------------------------------------

function pushClientConfig() {
  worker.postMessage({
    type: "config",
    qps: Number(qpsInput.value),
    clientDeadlineMs: Number(clientDeadlineInput.value),
    retryStrategy: retryStrategyInput.value,
    maxRetries: Number(maxRetriesInput.value),
  });
}

toggleBtn.onclick = () => {
  running = !running;
  if (running) {
    pushClientConfig();
    worker.postMessage({ type: "start" });
    toggleBtn.textContent = "Stop load";
    toggleBtn.classList.add("stop");
  } else {
    worker.postMessage({ type: "stop" });
    toggleBtn.textContent = "Start load";
    toggleBtn.classList.remove("stop");
  }
};

qpsInput.onchange = pushClientConfig;
clientDeadlineInput.onchange = pushClientConfig;
retryStrategyInput.onchange = pushClientConfig;
maxRetriesInput.onchange = pushClientConfig;

spikeLoadBtn.onclick = () => {
  worker.postMessage({ type: "spike", factor: 4, durationMs: SPIKE_MS });
  flash(spikeLoadBtn);
};

spikeLatencyBtn.onclick = async () => {
  try {
    await fetch("/api/chaos/latency", { method: "POST" });
    flash(spikeLatencyBtn);
  } catch (err) {
    /* ignore */
  }
};

function flash(btn) {
  btn.classList.add("active");
  setTimeout(() => btn.classList.remove("active"), SPIKE_MS);
}

clearQueueBtn.onclick = async () => {
  try {
    await fetch("/api/admin/clear-queue", { method: "POST" });
    clearQueueBtn.classList.add("active");
    setTimeout(() => clearQueueBtn.classList.remove("active"), 600);
  } catch (err) {
    /* ignore */
  }
};

// --- Sampling + rendering ---------------------------------------------------

setInterval(() => {
  push(goodputHistory, completedThisSecond);
  push(tputHistory, Math.round(server.throughput_ps));
  push(queueHistory, server.queue_len);
  render(completedThisSecond);
  completedThisSecond = 0;
}, 1000);

function push(arr, v) {
  arr.push(v);
  if (arr.length > 60) arr.shift();
}

function render(goodput) {
  setText("m-goodput", goodput);
  setText("m-tput", Math.round(server.throughput_ps));
  setText("m-qps", Math.round(server.start_work_qps + server.get_op_qps));
  setText("m-fail", Math.round(server.rpc_failure_ps));
  setText("m-inflight", server.in_flight);
  setText("m-queue", server.queue_len);
  drawChart(mainCanvas, [
    { data: tputHistory, color: "#58a6ff" },
    { data: goodputHistory, color: "#3fb950" },
  ]);
  drawChart(queueCanvas, [{ data: queueHistory, color: "#d29922" }]);
}

function setText(id, value) {
  el(id).textContent = value;
}

// --- Charts -----------------------------------------------------------------

const mainCanvas = el("main-chart");
const queueCanvas = el("queue-chart");

function drawChart(canvas, series) {
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const pad = 4;
  let max = 10;
  series.forEach((s) => s.data.forEach((v) => (max = Math.max(max, v))));
  const stepX = (w - pad * 2) / 59;
  const scaleY = (h - pad * 2) / max;

  series.forEach((s) => {
    if (s.data.length < 2) return;
    ctx.beginPath();
    s.data.forEach((v, i) => {
      const x = pad + i * stepX;
      const y = h - pad - v * scaleY;
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 2;
    ctx.stroke();
    ctx.lineTo(pad + (s.data.length - 1) * stepX, h - pad);
    ctx.lineTo(pad, h - pad);
    ctx.closePath();
    ctx.fillStyle = s.color + "1f"; // ~12% alpha
    ctx.fill();
  });
}

// --- Scenario presets -------------------------------------------------------

// Each preset snaps every knob (client + frontend + backend) to tell one story.
// autoSpike fires the matching chaos a few seconds in, so it's truly one-click.
const PRESETS = [
  {
    name: "Healthy baseline",
    desc: "Normal load at ~75% capacity — no retries, no shedding. Everything green.",
    client: { qps: 60, clientDeadlineMs: 1000, retryStrategy: "none", maxRetries: 0 },
    config: { shed_mode: "none", max_qps: 0, inflight_quota: 0, queue_mode: "FIFO", queue_max: 0, deadline_mode: "none", margin_ms: 70, latency_mode: "uniform", latency_ms: 50 },
  },
  {
    name: "Retry storm → collapse",
    desc: "Immediate retries, no shedding. Auto-spikes in 4s — goodput collapses and STAYS down after the spike ends (metastable). The load is gone but the system is still dead.",
    client: { qps: 60, clientDeadlineMs: 1000, retryStrategy: "immediate", maxRetries: 5 },
    config: { shed_mode: "none", max_qps: 0, inflight_quota: 0, queue_mode: "FIFO", queue_max: 0, deadline_mode: "none", margin_ms: 70, latency_mode: "uniform", latency_ms: 50 },
    autoSpike: "load",
  },
  {
    name: "Shed & survive",
    desc: "Same aggressive client, but the frontend sheds beyond 70 in-flight (429 = stop, not retry). Auto-spikes in 4s — goodput holds at capacity and recovers instantly.",
    client: { qps: 60, clientDeadlineMs: 1000, retryStrategy: "immediate", maxRetries: 5 },
    config: { shed_mode: "inflight", max_qps: 0, inflight_quota: 70, queue_mode: "FIFO", queue_max: 0, deadline_mode: "none", margin_ms: 70, latency_mode: "uniform", latency_ms: 50 },
    autoSpike: "load",
  },
  {
    name: "Deadline + margin",
    desc: "No shedding, but the backend refuses work it can't finish in time. Auto-spikes in 4s — queue stays bounded and goodput survives (the backend-side cure).",
    client: { qps: 60, clientDeadlineMs: 1000, retryStrategy: "none", maxRetries: 0 },
    config: { shed_mode: "none", max_qps: 0, inflight_quota: 0, queue_mode: "FIFO", queue_max: 0, deadline_mode: "margin", margin_ms: 70, latency_mode: "uniform", latency_ms: 50 },
    autoSpike: "load",
  },
];

let presetSpikeTimer = null;

async function applyPreset(p, btn) {
  if (presetSpikeTimer) clearTimeout(presetSpikeTimer);

  // Reset server state so each scenario starts from a clean slate.
  try {
    await fetch("/api/admin/clear-queue", { method: "POST" });
  } catch (err) {
    /* ignore */
  }

  // Reflect the preset in every control, then push it to server + worker.
  qpsInput.value = p.client.qps;
  clientDeadlineInput.value = p.client.clientDeadlineMs;
  retryStrategyInput.value = p.client.retryStrategy;
  maxRetriesInput.value = p.client.maxRetries;
  for (const k in p.config) {
    if (cfgInputs[k]) cfgInputs[k].value = p.config[k];
  }
  await postConfig();
  pushClientConfig();

  if (!running) {
    running = true;
    worker.postMessage({ type: "start" });
    toggleBtn.textContent = "Stop load";
    toggleBtn.classList.add("stop");
  }

  el("preset-desc").textContent = p.desc;
  document.querySelectorAll("#presets button").forEach((b) => b.classList.remove("active"));
  if (btn) btn.classList.add("active");

  if (p.autoSpike === "load") {
    presetSpikeTimer = setTimeout(() => {
      worker.postMessage({ type: "spike", factor: 4, durationMs: SPIKE_MS });
      flash(spikeLoadBtn);
    }, 4000);
  } else if (p.autoSpike === "latency") {
    presetSpikeTimer = setTimeout(async () => {
      try {
        await fetch("/api/chaos/latency", { method: "POST" });
      } catch (err) {
        /* ignore */
      }
      flash(spikeLatencyBtn);
    }, 4000);
  }
}

const presetsRow = el("presets");
PRESETS.forEach((p) => {
  const b = document.createElement("button");
  b.className = "preset";
  b.textContent = p.name;
  b.onclick = () => applyPreset(p, b);
  presetsRow.appendChild(b);
});

loadConfig();
