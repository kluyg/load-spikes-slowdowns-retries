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

**Phase 0** — end-to-end skeleton: one request type flows all the way through
and live goodput renders in the browser. Tunable knobs (retry strategies, load
shedding, queue strategy/size, deadline propagation, latency distributions),
the full metric stream, and the spike buttons arrive in later phases.
