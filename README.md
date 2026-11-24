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

### Rooms (multi-user isolation)

Every browser gets an isolated **room** — its id lives in the URL hash
(`#room=<id>`), so a room is bookmarkable and shareable. All of a room's state
lives under `room:<id>:` Redis keys (queue, config, metrics, completion bus),
and — crucially — the backend runs a **dedicated worker pool per room**. Sharing
capacity would ruin the whole point (the ~80 ops/s number only means anything
per-tenant), so two people driving the deployed system at once get fully
independent systems: separate knobs, separate graphs, separate capacity.

A room stays alive while the frontend keeps its entry fresh in a `rooms` sorted
set (on every RPC and each metric-stream tick). The backend polls that set and
spins a pool up when a room appears, tearing it down a few seconds after the tab
goes away; a frontend janitor then deletes the room's keys. Open the page twice
for two independent systems, or share a `#room=` link to watch one together.
Tabs sharing a room share its backend knobs: a change in one tab is pushed to
the others over the metric stream, and each change writes only the knob that
was touched. The client knobs (QPS, deadline, retries) stay per tab, because
every tab is its own client — each one that starts load adds to the room's
total.

The frontend allows at most `MAX_ROOMS` rooms at once (default 20, a rough
figure for one shared vCPU). A new room past the cap gets a 503 on every
endpoint and its page waits, retrying, until a room frees up; rooms that
already exist are never turned away.

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

**Client latency (p50 / p99)** is also measured in the client: the time from a
logical request starting to its answer arriving, including every failed attempt
and backoff sleep in between. Percentiles are taken over each one-second window
of *successful* requests, so a second in which nothing succeeds shows as a gap
rather than a number. Because the client polls every 250 ms, observed latency
moves in roughly 250 ms steps and never drops below one poll interval.

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

## Reproducing the numbers

The tables above come from [`loadgen/`](loadgen/), a headless twin of the
browser client: the same `StartWork` + poll loop and retry policy as
`static/worker.js`, driven from Go through scripted scenarios (5 s warmup at
60/s, a 10 s spike, 15 s cooldown). With the stack up:

```sh
cd loadgen
go run . -list                       # the scenarios
go run . -format markdown            # run them all, print README-shaped tables
go run . -scenario fifo,lifo,margin  # or just some; -format json adds a per-second timeline
go run . -check                      # exit 1 if a claim above stops holding
```

Each scenario runs in its own fresh room, so runs don't contaminate each other
or anyone using the UI. `-check` asserts the *shapes* (LIFO and deadline + margin
beat FIFO, retries amplify and never recover, clearing the queue doesn't help,
shedding pins the queue and recovers), not the absolute numbers, and CI runs it
against the real `docker compose` stack on every push.

## Implementation notes

- **Pub/sub is a fast path, never a source of truth.** `GetOperation` falls back
  to a durable `op:<id>` key, and in-flight / throughput are durable Redis
  counters — because Redis pub/sub is at-most-once and drops messages under the
  exact overload these metrics exist to measure.
- **Redis is the only broker:** work queue, completion bus, config, and metrics
  all live in it, which keeps the moving parts minimal.
- **Rooms are just a key prefix plus a pool per room.** Isolation costs almost
  nothing structurally — every room's state is `room:<id>:…` keys and the
  backend scales workers with the number of live rooms. Idle rooms are garbage
  collected; the only thing left to expire on its own is the per-op `done`
  marker, which lives until the op's deadline plus 60 s — once the client has
  stopped polling, nobody can ask for it.
- **Determinism caveat:** this is a real system, not a simulation, so absolute
  numbers vary run to run; the *shapes* and orderings are the point.
