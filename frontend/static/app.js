// Main-thread controller: owns the worker (client tier), measures client-side
// goodput, consumes the server's SSE metric stream, drives the live config
// panel, and renders the dashboard.

const SPIKE_MS = 8000; // keep in sync with backend chaosDuration

// --- Room ------------------------------------------------------------------
// Every browser gets an isolated room: its own queue, config, metrics, and
// backend capacity. The id lives in the URL hash so a room is bookmarkable and
// shareable — open the same #room= link on two tabs to share one experiment,
// or just load the page twice to get two independent systems.
function roomId() {
  const m = location.hash.match(/room=([A-Za-z0-9_-]{1,64})/);
  if (m) return m[1];
  const id = Array.from(crypto.getRandomValues(new Uint8Array(6)))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  location.hash = "room=" + id;
  return id;
}
const ROOM = roomId();

// withRoom appends the room id to an API path so every RPC lands in this room.
function withRoom(path) {
  return path + (path.includes("?") ? "&" : "?") + "room=" + encodeURIComponent(ROOM);
}

const worker = new Worker("/worker.js");
worker.postMessage({ type: "config", room: ROOM }); // hand the room to the client tier

// Client-observed goodput (requests that completed before their deadline).
let completedThisSecond = 0;
// Client-observed latencies (ms) of the logical requests that completed this
// second. Percentiles are taken over this one-second window, like goodput.
let latenciesThisSecond = [];
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
// null marks a second with no completions: there is no latency to report, and
// drawing 0 there would make a collapse look fast.
const p50History = [];
const p99History = [];

worker.onmessage = (e) => {
  if (e.data.type === "completed") {
    completedThisSecond++;
    if (e.data.latencyMs != null) latenciesThisSecond.push(e.data.latencyMs);
  }
};

// --- Server metric stream (SSE) --------------------------------------------

// The stream opens with a "hello" naming the room's incarnation. EventSource
// reconnects by itself after a network drop or laptop sleep, and the server
// deletes a room that has been idle for ~20 s — so a reconnect may land in a
// fresh room running default config while the knobs here still show the old
// values. A changed incarnation means exactly that: push the knobs back so the
// server matches what's on screen. Same incarnation means the room survived;
// the stream's "config" event then brings the knobs up to date.
//
// "config" is how tabs sharing a room stay in step: the server sends the room's
// knobs when the stream opens and again whenever anyone changes one. "viewers"
// is the number of tabs currently watching the room.
let roomBornMs = null;

function connectStream() {
  const es = new EventSource(withRoom("/api/stream"));
  es.onmessage = (e) => {
    try {
      server = JSON.parse(e.data);
    } catch (err) {
      /* ignore malformed frame */
    }
  };
  es.addEventListener("hello", (e) => {
    let born;
    try {
      born = JSON.parse(e.data).born_ms;
    } catch (err) {
      return;
    }
    const first = roomBornMs === null;
    const changed = !first && born !== roomBornMs;
    roomBornMs = born;
    if (changed) postConfig();
  });
  es.addEventListener("config", (e) => {
    try {
      applyConfig(JSON.parse(e.data));
    } catch (err) {
      /* ignore malformed frame */
    }
  });
  es.addEventListener("viewers", (e) => showViewers(Number(e.data)));
  es.onerror = () => {
    // The browser retries a dropped connection by itself, but gives up for good
    // on an HTTP error — which is how the server answers a new room when it is
    // already running as many as it allows. Take over and wait for a free one.
    if (es.readyState === EventSource.CLOSED) waitForRoom();
  };
}

// --- Waiting for a free room -----------------------------------------------
// The server caps the number of live rooms. A new room past the cap gets a 503
// marked X-Rooms-Full on every endpoint; rooms free up ~20 s after their tab
// closes. Say so, probe until we're let in, then bring this room up with the
// knobs on screen.
const ROOM_RETRY_MS = 5000;
let waitingForRoom = false;

async function waitForRoom() {
  if (waitingForRoom) return;
  waitingForRoom = true;
  const notice = document.getElementById("rooms-full");
  for (;;) {
    try {
      const res = await fetch(withRoom("/api/config"));
      if (res.ok) break;
      notice.hidden = res.headers.get("X-Rooms-Full") !== "1";
    } catch (err) {
      /* server unreachable: keep trying quietly */
    }
    await new Promise((r) => setTimeout(r, ROOM_RETRY_MS));
  }
  notice.hidden = true;
  waitingForRoom = false;
  roomBornMs = null; // whatever we connect to now is a room we just created
  await postConfig();
  connectStream();
}

connectStream();

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

// The server's knobs are the truth for the whole room, so every way of learning
// them — the initial load, a stream "config" event, the answer to our own POST
// — goes through applyConfig.
let lastConfig = null;
let pushing = 0; // config POSTs in flight

function applyConfig(c) {
  lastConfig = c;
  // While our own change is on its way the server may still report the old
  // value; its answer (or the next "config" event) will carry the merged result.
  if (pushing > 0) return;
  for (const key in cfgInputs) {
    const input = cfgInputs[key];
    if (c[key] === undefined) continue;
    // Don't yank a field out from under someone typing in it (focused, and
    // holding a value they haven't committed); it catches up when they commit
    // or leave it.
    if (input === document.activeElement && input.value !== input.dataset.synced) continue;
    input.value = c[key];
    input.dataset.synced = input.value;
  }
}

async function loadConfig() {
  try {
    const res = await fetch(withRoom("/api/config"));
    if (res.ok) applyConfig(await res.json());
  } catch (err) {
    /* keep the HTML defaults */
  }
}

// postConfig sends the named knobs, or all of them when called with none. A
// single edit sends only its own knob: in a shared room the others may have
// been changed by someone else since this tab last heard, and must not be
// overwritten with what happens to be on screen here.
async function postConfig(keys) {
  const body = {};
  for (const key of keys || Object.keys(cfgInputs)) {
    const input = cfgInputs[key];
    body[key] = input.type === "number" ? Number(input.value) : input.value;
    input.dataset.synced = input.value; // committed: no longer an edit in progress
  }
  pushing++;
  let merged = null;
  try {
    const res = await fetch(withRoom("/api/config"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (res.ok) merged = await res.json();
  } catch (err) {
    /* ignore */
  }
  pushing--;
  if (merged) applyConfig(merged);
}

for (const key in cfgInputs) {
  const input = cfgInputs[key];
  input.dataset.synced = input.value;
  input.onchange = () => postConfig([key]);
  // Leaving a field without changing it: show whatever arrived meanwhile.
  input.onblur = () => {
    if (lastConfig) setTimeout(() => applyConfig(lastConfig), 0);
  };
}

function showViewers(n) {
  const note = el("room-viewers");
  note.hidden = !(n > 1);
  note.textContent =
    " " + n + " tabs are in this room. Backend knobs are shared between them; each tab that starts load adds its own.";
}

// --- Controls ---------------------------------------------------------------

function pushClientConfig() {
  worker.postMessage({
    type: "config",
    room: ROOM,
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
    await fetch(withRoom("/api/chaos/latency"), { method: "POST" });
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
    await fetch(withRoom("/api/admin/clear-queue"), { method: "POST" });
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
  const sorted = latenciesThisSecond.sort((a, b) => a - b);
  const p50 = percentile(sorted, 50);
  const p99 = percentile(sorted, 99);
  push(p50History, p50);
  push(p99History, p99);
  render(completedThisSecond, p50, p99);
  completedThisSecond = 0;
  latenciesThisSecond = [];
}, 1000);

// percentile uses the nearest-rank method on an ascending array: the smallest
// sample with at least p% of the samples at or below it. Returns null when
// there are no samples. With fewer than 100 samples p99 is simply the max.
function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const rank = Math.ceil((p / 100) * sorted.length);
  return Math.round(sorted[Math.max(rank, 1) - 1]);
}

function push(arr, v) {
  arr.push(v);
  if (arr.length > 60) arr.shift();
}

function render(goodput, p50, p99) {
  setText("m-goodput", goodput);
  setText("m-tput", Math.round(server.throughput_ps));
  setText("m-qps", Math.round(server.start_work_qps + server.get_op_qps));
  setText("m-fail", Math.round(server.rpc_failure_ps));
  setText("m-inflight", server.in_flight);
  setText("m-queue", server.queue_len);
  setText("m-p50", p50 == null ? "\u2013" : p50);
  setText("m-p99", p99 == null ? "\u2013" : p99);
  drawChart(mainCanvas, [
    { data: tputHistory, color: "#58a6ff" },
    { data: goodputHistory, color: "#3fb950" },
  ], "ops/s");
  drawChart(queueCanvas, [{ data: queueHistory, color: "#d29922" }], "items");
  drawChart(latencyCanvas, [
    { data: p99History, color: "#f778ba" },
    { data: p50History, color: "#a371f7" },
  ], "ms");
}

function setText(id, value) {
  el(id).textContent = value;
}

// --- Charts -----------------------------------------------------------------

const mainCanvas = el("main-chart");
const queueCanvas = el("queue-chart");
const latencyCanvas = el("latency-chart");

// niceCeil rounds up to a 1/2/5 x 10^n value so axis ticks land on round numbers.
function niceCeil(v) {
  const pow = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 2, 5, 10]) {
    if (m * pow >= v) return m * pow;
  }
  return 10 * pow;
}

function drawChart(canvas, series, unit) {
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  let rawMax = 10;
  series.forEach((s) => s.data.forEach((v) => {
    if (v != null) rawMax = Math.max(rawMax, v);
  }));
  const max = niceCeil(rawMax);

  ctx.font = "10px -apple-system, 'Segoe UI', sans-serif";

  // Plot area: size the left gutter to the widest y label so it never clips.
  const topLabel = `${max} ${unit}`;
  const padL = Math.ceil(ctx.measureText(topLabel).width) + 10;
  const padR = 6, padT = 6, padB = 16;
  const plotW = w - padL - padR;
  const plotH = h - padT - padB;
  const stepX = plotW / 59;
  const scaleY = plotH / max;

  // Y axis: gridlines + tick labels at 0 / half / max, unit on the top tick.
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  [0, max / 2, max].forEach((v) => {
    const y = padT + plotH - v * scaleY;
    ctx.strokeStyle = "#262d38";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(w - padR, y);
    ctx.stroke();
    ctx.fillStyle = "#8b95a4";
    const label = v === max ? `${v} ${unit}` : `${v}`;
    ctx.fillText(label, padL - 5, Math.max(y, padT + 5));
  });

  // X axis: time, oldest sample on the left.
  ctx.textBaseline = "top";
  const xTicks = [
    { i: 0, label: "-60s", align: "left" },
    { i: 30, label: "-30s", align: "center" },
    { i: 59, label: "now", align: "right" },
  ];
  ctx.fillStyle = "#8b95a4";
  xTicks.forEach((t) => {
    ctx.textAlign = t.align;
    ctx.fillText(t.label, padL + t.i * stepX, padT + plotH + 4);
  });

  series.forEach((s) => {
    // Newest sample pinned at the right edge; the series grows in from the right.
    const offset = 60 - s.data.length;
    const xAt = (i) => padL + (offset + i) * stepX;
    const yAt = (v) => padT + plotH - v * scaleY;

    // A null sample means "no data that second": split the series into runs of
    // real samples and leave a gap between them rather than drawing through.
    const runs = [];
    let run = null;
    s.data.forEach((v, i) => {
      if (v == null) {
        run = null;
        return;
      }
      if (!run) runs.push((run = []));
      run.push(i);
    });

    runs.forEach((idx) => {
      if (idx.length === 1) {
        // An isolated sample has no neighbour to connect to; show it as a dot.
        ctx.beginPath();
        ctx.arc(xAt(idx[0]), yAt(s.data[idx[0]]), 2, 0, 2 * Math.PI);
        ctx.fillStyle = s.color;
        ctx.fill();
        return;
      }
      ctx.beginPath();
      idx.forEach((i, n) => {
        const x = xAt(i);
        const y = yAt(s.data[i]);
        n === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2;
      ctx.stroke();
      ctx.lineTo(xAt(idx[idx.length - 1]), padT + plotH);
      ctx.lineTo(xAt(idx[0]), padT + plotH);
      ctx.closePath();
      ctx.fillStyle = s.color + "1f"; // ~12% alpha
      ctx.fill();
    });
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
    await fetch(withRoom("/api/admin/clear-queue"), { method: "POST" });
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
        await fetch(withRoom("/api/chaos/latency"), { method: "POST" });
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

// Show the room id and a shareable link to this exact experiment.
el("room-id").textContent = ROOM;
const roomLink = el("room-link");
roomLink.href = location.origin + location.pathname + "#room=" + ROOM;

loadConfig();
