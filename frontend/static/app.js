// Main-thread controller: owns the worker (client tier), measures client-side
// goodput, consumes the server's SSE metric stream, and renders the dashboard.

const SPIKE_MS = 8000; // keep in sync with backend chaosDuration

const worker = new Worker("/worker.js");

// Client-observed counters.
let completed = 0;
let completedThisSecond = 0;
let running = false;

// Latest server-side snapshot from the SSE stream.
let server = {
  start_work_qps: 0,
  get_op_qps: 0,
  rpc_success_ps: 0,
  rpc_failure_ps: 0,
  in_flight: 0,
  queue_len: 0,
};

const goodputHistory = [];
const queueHistory = [];

worker.onmessage = (e) => {
  if (e.data.type === "completed") {
    completed++;
    completedThisSecond++;
  }
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

// --- Controls ---------------------------------------------------------------

const toggleBtn = document.getElementById("toggle");
const qpsInput = document.getElementById("qps");
const spikeLoadBtn = document.getElementById("spike-load");
const spikeLatencyBtn = document.getElementById("spike-latency");

toggleBtn.onclick = () => {
  running = !running;
  if (running) {
    worker.postMessage({ type: "config", qps: Number(qpsInput.value) });
    worker.postMessage({ type: "start" });
    toggleBtn.textContent = "Stop load";
    toggleBtn.classList.add("stop");
  } else {
    worker.postMessage({ type: "stop" });
    toggleBtn.textContent = "Start load";
    toggleBtn.classList.remove("stop");
  }
};

qpsInput.onchange = () => {
  worker.postMessage({ type: "config", qps: Number(qpsInput.value) });
};

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

// Show a button as "active" for the spike duration.
function flash(btn) {
  btn.classList.add("active");
  setTimeout(() => btn.classList.remove("active"), SPIKE_MS);
}

// --- Sampling + rendering ---------------------------------------------------

// Goodput is measured here, in the client, because this is where a request is
// actually observed to succeed from the user's point of view.
setInterval(() => {
  push(goodputHistory, completedThisSecond);
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
  setText("m-qps", Math.round(server.start_work_qps + server.get_op_qps));
  setText("m-success", Math.round(server.rpc_success_ps));
  setText("m-fail", Math.round(server.rpc_failure_ps));
  setText("m-inflight", server.in_flight);
  setText("m-queue", server.queue_len);
  drawChart(goodputCanvas, goodputHistory, "#3fb950");
  drawChart(queueCanvas, queueHistory, "#d29922");
}

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

// --- Charts -----------------------------------------------------------------

const goodputCanvas = document.getElementById("goodput-chart");
const queueCanvas = document.getElementById("queue-chart");

function drawChart(canvas, data, color) {
  const ctx = canvas.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const n = data.length;
  if (n < 2) return;
  const max = Math.max(10, ...data);
  const pad = 4;
  const stepX = (w - pad * 2) / 59;
  const scaleY = (h - pad * 2) / max;

  ctx.beginPath();
  data.forEach((v, i) => {
    const x = pad + i * stepX;
    const y = h - pad - v * scaleY;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.stroke();

  ctx.lineTo(pad + (n - 1) * stepX, h - pad);
  ctx.lineTo(pad, h - pad);
  ctx.closePath();
  ctx.fillStyle = color + "20"; // ~12% alpha fill
  ctx.fill();
}
