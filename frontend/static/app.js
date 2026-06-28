// Main-thread controller: owns the worker (client tier), aggregates the events
// it emits into live metrics, polls the frontend for server-side metrics, and
// renders the dashboard.

const worker = new Worker("/worker.js");

let started = 0;
let completed = 0;
let failed = 0;
let completedThisSecond = 0;
let queueLen = 0;
let running = false;

const goodputHistory = []; // last 60 one-second samples

worker.onmessage = (e) => {
  switch (e.data.type) {
    case "started":
      started++;
      break;
    case "completed":
      completed++;
      completedThisSecond++;
      break;
    case "failed":
      failed++;
      break;
  }
};

// --- Controls ---------------------------------------------------------------

const toggleBtn = document.getElementById("toggle");
const qpsInput = document.getElementById("qps");

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
  if (running) worker.postMessage({ type: "start" }); // re-arm at new rate
};

// --- Metric sampling --------------------------------------------------------

// Goodput is measured here, in the client, because this is where a request is
// actually observed to succeed from the user's point of view.
setInterval(() => {
  goodputHistory.push(completedThisSecond);
  if (goodputHistory.length > 60) goodputHistory.shift();
  render(completedThisSecond);
  completedThisSecond = 0;
}, 1000);

// Server-side metrics the browser can't see for itself.
setInterval(async () => {
  try {
    const res = await fetch("/api/metrics");
    const m = await res.json();
    queueLen = m.queue_len ?? 0;
  } catch (err) {
    /* ignore transient errors */
  }
}, 500);

// --- Rendering --------------------------------------------------------------

function render(goodput) {
  setText("m-goodput", goodput);
  setText("m-started", started);
  setText("m-completed", completed);
  setText("m-failed", failed);
  setText("m-inflight", Math.max(0, started - completed - failed));
  setText("m-queue", queueLen);
  drawChart();
}

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

const canvas = document.getElementById("goodput-chart");
const ctx = canvas.getContext("2d");

function drawChart() {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const max = Math.max(10, ...goodputHistory);
  const n = goodputHistory.length;
  if (n < 2) return;

  const pad = 4;
  const stepX = (w - pad * 2) / 59;
  const scaleY = (h - pad * 2) / max;

  ctx.beginPath();
  goodputHistory.forEach((v, i) => {
    const x = pad + i * stepX;
    const y = h - pad - v * scaleY;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#3fb950";
  ctx.lineWidth = 2;
  ctx.stroke();

  // soft fill under the line
  ctx.lineTo(pad + (n - 1) * stepX, h - pad);
  ctx.lineTo(pad, h - pad);
  ctx.closePath();
  ctx.fillStyle = "rgba(63, 185, 80, 0.12)";
  ctx.fill();
}
