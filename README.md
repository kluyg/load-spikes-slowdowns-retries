# Load Spikes, Slowdowns & Retries

An interactive, runnable distributed system for *seeing* how load, retries,
latency, and load-shedding policies interact — and what they do to **goodput**
(the rate of requests that actually succeed end-to-end).

Builds on the ideas in [Shed your load](https://strebkov.dev/posts/shed-your-load/).

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

- **Browser clients** — Web Workers that issue real RPCs and host the retry logic.
- **Go frontend** — serves the UI, exposes `StartWork` / `GetOperation`, applies
  load shedding (later phase), enqueues work, learns of completions via pub/sub.
- **Go backend** — worker pool consuming the queue, simulating work latency.
- **Redis** — the single backbone: work queue (list), completion bus (pub/sub),
  and later config + metrics.

## Run

```sh
docker compose up --build
```

Then open <http://localhost:8080>. The fastest tour is the **Scenarios** row at
the top — each is one click, snaps every knob, and auto-fires the spike:

1. **Retry storm → collapse** — watch goodput crater and stay down after the spike.
2. **Shed & survive** — same aggressive client, but load shedding holds goodput up.
3. **Deadline + margin** — the backend-side cure: bounded queue, goodput survives.

Or drive the knobs yourself with **Start load** and the panels below.

## Status

**Client retries → metastable collapse** — the client tier supports retry
strategies (none / immediate / exponential backoff / backoff + jitter) and a max
retry count. Under overload, retries manufacture new load: a short spike tips the
system into a self-sustaining collapse that persists *after* the spike ends —
the load is gone but the system stays dead.

Measured (60 qps baseline, 8s spike to 5× capacity, FIFO, no deadline drop):

| Client policy | Offered amplification | After the spike ends |
|---|---|---|
| No retry | 1× (3,775 attempts) | queue drains — system recovers |
| Immediate retry ×5 | **5.3× (18,278 attempts)** | offered stuck at 300–1250/s, queue 5k→15k+, never recovers |

There's an operator **"Clear queue"** lever that drops the whole backlog
(orphaned operations then 404 on their next poll, and clients retry fresh). It
does **not** fix the collapse: the 404'd operations all retry at once — a
thundering herd that re-saturates the queue within a second or two. That's the
point — a metastable collapse is sustained by *client* retry behavior, so a
server-side flush is a band-aid the retry storm tears off.

**Load shedding — the cure** — the frontend can shed load (none / fixed max QPS
shared across both RPCs / in-flight quota), returning **429**. Crucially the
client treats a 429 as backpressure — it gives up rather than retrying — so
shedding both protects the backend *and* stops the retry storm from forming.

Same immediate-retry spike as above, with an in-flight quota of 70:

| | Shedding off | In-flight quota = 70 |
|---|---|---|
| Queue during spike | explodes → 13,800+ | pinned at 70 |
| Goodput during spike | 0 | ~78/s (≈ capacity) |
| After the spike | collapsed indefinitely | drains, recovers in ~3s |
| Offered amplification | 5.25× | 1× (no retry storm) |
| Total goodput | 12% | 59% |

The full arc: backend knobs → retries cause a metastable collapse → clearing the
queue can't fix it → load shedding with a well-behaved client does.

**Scenario presets** (the **Scenarios** row in the UI) snap every knob — client,
frontend, and backend — to one configuration and auto-fire the spike, so the
whole arc is reproducible in one click each.

Still to come: backoff + jitter as a client-side mitigation.

**Backend knobs** — live-tunable backend behavior that reproduces the
throughput-vs-goodput dynamics from
[Shed your load](https://strebkov.dev/posts/shed-your-load/):

- **Queue strategy:** FIFO (serve oldest) vs LIFO (serve newest)
- **Queue max size:** unbounded or a fixed cap (enqueue-time shedding)
- **Deadline propagation:** none / drop if past deadline / drop if `now + margin >
  deadline` ("refuse it now rather than discover the waste after")
- **Work latency:** uniform vs long-tail (mean-preserving lognormal)

Each client request carries a **deadline**; if the backend can't satisfy it the
client gives up, which is what makes **goodput** (useful, client-still-waiting
completions) diverge from **throughput** (work done). The hero chart overlays the
two.

Measured with a headless load generator (60 qps baseline, 10s spike to 5×
capacity, 1s deadline):

| Scenario | Goodput (of offered) | Peak queue |
|---|---|---|
| FIFO, no deadline drop | 10% | 1953 |
| LIFO, no deadline drop | 51% | 1929 |
| FIFO, deadline + margin | 52% | 263 |

Still to come: frontend load shedding (the cure for the retry storm) and
one-click scenario presets.

<details>
<summary>Earlier phases</summary>

- **Phase 1** — all five server metrics stream over Server-Sent Events; chaos
  buttons for a 4× load spike and a 4× backend-latency spike.
- **Phase 0** — end-to-end skeleton: one request flows browser → frontend → work
  queue → backend → completion → GetOperation, with live goodput.
</details>
