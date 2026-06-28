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

Then open <http://localhost:8080> and click **Start load**.

You should see goodput climb to roughly your target QPS, the queue length stay
near zero, and in-flight settle to a small number — a healthy system.

## Status

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

Still to come: client retry strategies (which produce the self-sustaining
metastable collapse), frontend load shedding, and one-click scenario presets.

<details>
<summary>Earlier phases</summary>

- **Phase 1** — all five server metrics stream over Server-Sent Events; chaos
  buttons for a 4× load spike and a 4× backend-latency spike.
- **Phase 0** — end-to-end skeleton: one request flows browser → frontend → work
  queue → backend → completion → GetOperation, with live goodput.
</details>
