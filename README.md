# Load Spikes, Slowdowns & Retries

An interactive, runnable distributed system for *seeing* how load, retries,
latency, and load-shedding policies interact — and what they do to **goodput**
(the rate of requests that actually succeed end-to-end, while a client is still
waiting for the answer).

Builds on the ideas in [Shed your load](https://strebkov.dev/posts/shed-your-load/),
but as a real deployed system you can drive in the browser rather than a
simulation.

## Architecture

```
[browser clients]  --StartWork/GetOperation-->  [Go frontend]  --LPUSH-->  (Redis list = work queue)
   (Web Workers)                                      ^                            |
   retry policy                                        |                       BRPOP/BLPOP
        |                                              |                            v
        |                                              |                      [Go backend]
        |                                         (Redis pub/sub)  <--PUBLISH--  simulated work
        +----------------- completion <----------------+
```

- **Browser clients** — Web Workers that issue real RPCs and host the retry
  logic. The same single page is the client tier, the control panel, and the
  dashboards.
- **Go frontend** — serves the UI, exposes `StartWork` / `GetOperation`, applies
  load shedding, enqueues work, and learns of completions via pub/sub.
- **Go backend** — worker pool consuming the queue, applying the queue/deadline
  policies and simulating work latency.
- **Redis** — the single backbone: work queue (list), completion bus (pub/sub),
  live config (hash), and metric counters.

The request flow is the Long-Running-Operation poll pattern: `StartWork` returns
an operation id; the client polls `GetOperation` until it's done or the client's
deadline passes. Capacity ≈ workers ÷ latency = 4 ÷ 50 ms ≈ **80 ops/s**.

## Run

```sh
docker compose up --build
```

No keys or external accounts needed — this is the full local stack. Then open
<http://localhost:8080>.

> Optional: `docker compose --profile tailnet up` also starts a Tailscale sidecar
> that serves the frontend on your tailnet (proxying to `frontend:8080` over the
> Docker network). It needs `TS_AUTHKEY` in a gitignored `.env`; stop it with
> `docker compose --profile tailnet down`. To host a durable public instance, see
> [DEPLOY.md](DEPLOY.md).

## Try it: scenarios

The fastest tour is the **Scenarios** row at the top. Each is one click — it
snaps every knob (client, frontend, backend), starts load, and auto-fires the
spike a few seconds in:

1. **Healthy baseline** — normal load at ~75% capacity. Everything green.
2. **Retry storm → collapse** — goodput craters and *stays down after the spike
   ends*. The load is gone but the system is still dead.
3. **Shed & survive** — same aggressive client, but load shedding holds goodput
   at capacity and recovers instantly.
4. **Deadline + margin** — the backend-side cure: bounded queue, goodput survives.

Or drive the knobs yourself with **Start load** and the panels below.

## What you can tune

- **Client:** target QPS · request deadline · retry strategy (none / immediate /
  exponential backoff / backoff + jitter) · max retries.
- **Frontend:** load shedding — none / fixed max QPS (a token bucket shared by
  both RPCs) / in-flight quota. Sheds with HTTP 429, which the client treats as
  backpressure (it gives up rather than retrying).
- **Backend:** queue strategy (FIFO / LIFO) · queue max size · deadline
  propagation (none / drop if past deadline / drop if `now + margin > deadline`) ·
  work latency (uniform / long-tail, a mean-preserving lognormal).
- **Chaos / ops:** 4× load spike · 4× backend-latency spike · operator
  "Clear queue".

## Metrics

Server-side metrics stream to the UI over Server-Sent Events: RPC QPS, RPC
success/failure rate, in-flight work, queue length, and **throughput** (work the
backend completed). **Goodput** is measured in the client, where success is
actually observed. The hero chart overlays throughput and goodput — under
overload, throughput stays flat while goodput falls off a cliff.

## What it demonstrates

The whole arc, each step reproducible from a preset and verified with a headless
load generator.

**1. Queue discipline and deadlines decide who survives overload.** A 10 s spike
to 5× capacity, 1 s client deadline, no retries:

| Backend policy | Goodput (of offered) | Peak queue |
|---|---|---|
| FIFO, no deadline drop | 10% | 1953 |
| LIFO, no deadline drop | 51% | 1929 |
| FIFO, deadline + margin | 52% | 263 |

FIFO serves work whose clients already gave up; LIFO serves the freshest;
deadline + margin refuses work it can't finish in time, rescuing goodput *and*
bounding the queue.

**2. Client retries turn a spike into a metastable collapse.** Same spike, FIFO,
no deadline drop:

| Client policy | Offered amplification | After the spike ends |
|---|---|---|
| No retry | 1× (3,775 attempts) | queue drains — system recovers |
| Immediate retry ×5 | **5.3× (18,278 attempts)** | offered stuck at 300–1250/s, queue 5k→15k+, never recovers |

Retries manufacture new load that sustains the overload after the trigger is
gone — a self-sustaining failure with no exit on its own.

**3. Clearing the queue does *not* fix it.** The operator "Clear queue" lever
drops the whole backlog; the orphaned operations 404 on their next poll and all
retry at once — a thundering herd that re-saturates the queue within seconds. A
metastable collapse is sustained by *client* behavior, so a server-side flush is
a band-aid the retry storm tears off.

**4. Load shedding is the cure.** Same immediate-retry spike, in-flight quota of 70:

| | Shedding off | In-flight quota = 70 |
|---|---|---|
| Queue during spike | explodes → 13,800+ | pinned at 70 |
| Goodput during spike | 0 | ~78/s (≈ capacity) |
| After the spike | collapsed indefinitely | drains, recovers in ~3s |
| Offered amplification | 5.25× | 1× (no retry storm) |
| Total goodput | 12% | 59% |

Shedding caps the queue at the quota so the backend always has fresh work it can
finish in time. Because the client honors the 429 (stops instead of retrying),
shedding also prevents the retry storm from forming in the first place.

## Implementation notes

- **Pub/sub is a fast path, never a source of truth.** `GetOperation` falls back
  to a durable `op:<id>` key, and in-flight / throughput are durable Redis
  counters — because Redis pub/sub is at-most-once and drops messages under the
  exact overload these metrics exist to measure.
- **Redis is the only broker:** work queue, completion bus, config, and metrics
  all live in it, which keeps the moving parts minimal.
- **Determinism caveat:** this is a real system, not a simulation, so absolute
  numbers vary run to run; the *shapes* and orderings are the point.
