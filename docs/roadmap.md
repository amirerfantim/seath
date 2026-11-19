# Seath — Roadmap

What is planned next. Earlier lists also had table-name validation, a counter for
dropped messages, CPU and memory gauges (the System page), whole-table CSV
export, unit tests and encrypted NATS credentials. Those are built, so they are
not listed here. The list below was checked against the code on 2026-11-19.

## Operations

1. **Single-writer guard.** The one-replica rule is only enforced by the
   deployment configuration today. Add a Postgres advisory lock
   (`pg_try_advisory_lock`) on boot so that a second `serve` process refuses to
   run recorders. This is defense in depth against an accidental scale-up or an
   overlapping rollout, and it pairs with the `Recreate` rollout strategy.
2. **Prometheus `/metrics`.** Export per-recording received, written, error and
   dropped counts, write-batch latency, NATS connection state and database pool
   statistics.
3. **Structured request logging with trace IDs** for the API.
4. **Key rotation.** Rotating `SEATH_SECRET_KEY` is manual today; see
   [deployment](deployment.md). Automate it.

## Features

5. **JetStream support.** Honor the `mode` setting of the NATS config (`core` or
   `jetstream`; it is parsed today but not used). Durable consumers and replay
   would let Seath backfill, not only tail.
6. **Column transforms.** Simple per-column expressions at extract time, for
   example epoch to timestamp, or enum to label.
7. **Recording templates and clone.** "Record this topic like that one."
