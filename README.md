# Resilient Event Processor

An HTTP-driven event processing system that guarantees **no event is lost and
no event is processed twice**, even when downstream processing fails. Failed
events are isolated in a recoverable dead-letter queue instead of being
dropped or silently retried forever, and an AI-assisted analysis step helps
diagnose *why* an event ended up there.

## Why this exists

Most "process an event" demos stop at the happy path. This project is about
the part that actually matters in production: what happens when processing
fails halfway through, when the same event arrives twice, or when a worker
crashes mid-retry. The core guarantees — idempotency under real concurrency,
retries that don't hammer a struggling downstream service, and failures that
are isolated instead of lost — are the focus, not a checklist of
infrastructure tools.

## Key guarantees

- **At-least-once ingestion, exactly-once processing.** Events are accepted
  over HTTP and queued immediately; a unique constraint on the event ID is
  what actually prevents duplicate side effects, not an application-level
  check.
- **Resilient retries.** Transient failures are retried with exponential
  backoff and jitter, instead of failing immediately or retrying in lockstep
  with every other worker.
- **Isolated failures, not lost ones.** Once retries are exhausted, an event
  moves to a dead-letter queue with its error and attempt history attached,
  instead of disappearing.
- **Recoverable, not just observable.** Dead-lettered events can be replayed
  back into the normal processing path once the underlying issue is fixed —
  they don't need a separate recovery mechanism, because the same
  idempotency guarantee makes replay safe.
- **AI-assisted diagnosis.** Each dead-lettered event can be analyzed to
  suggest a likely root cause, instead of requiring a human to read raw
  error logs first.

## Architecture

![Architecture diagram: an event moves from HTTP ingestion through Kafka, a worker pool, an idempotency check backed by a unique constraint, a retry loop with backoff and jitter, and — on exhausted retries — into a dead-letter queue that supports replay and AI-assisted analysis.](docs/architecture.svg)

The system is built around one decision: **accepting** an event and
**processing** it are decoupled by a message queue. That's what makes an
immediate `202 Accepted` response possible, what lets a slow or failing
worker be retried without blocking ingestion, and what makes replay "free" —
a replayed event just re-enters the same pipeline, going through the same
idempotency check as any other event.

The database has three tables, each answering one operational question:

| Table | Question it answers |
|---|---|
| `processed_events` | Has this event already been processed? |
| `dlq_events` | What failed, and why? |
| `dlq_analysis` | What does the AI think caused it? |

## Tech stack

| Layer | Choice |
|---|---|
| Backend | Go |
| Event streaming | Kafka (KRaft mode) |
| Database | PostgreSQL |
| Integration testing | Testcontainers |
| Metrics | Prometheus + Grafana |
| Frontend | Next.js + TanStack Query |
| Failure analysis | Ollama (local LLM) |

## Project status

This project is being built in phases, each one shipped and tested before
the next begins.

- [ ] **Phase 1 — Core: events that don't get lost.** HTTP ingestion, Kafka
      pipeline, idempotency, retry with backoff + jitter, dead-letter queue,
      integration tests proving all of it under real concurrency.
- [ ] **Phase 2 — Visibility and recovery.** Prometheus metrics, Grafana
      dashboard, DLQ listing and replay endpoints.
- [ ] **Phase 3 — Interface and AI.** Minimal frontend for browsing and
      replaying dead-lettered events, AI-assisted root-cause analysis.

## Non-goals (for now)

Deliberately out of scope for this project: Kubernetes, GitOps/ArgoCD,
full distributed tracing, load testing, mutation testing, contract testing,
end-to-end test automation, multi-tenancy, and complex auth. These are real
concerns in larger systems, but adding them here would trade depth on the
guarantees above for breadth across tools. They may become a v2.

## Getting started

Coming in Phase 1: a `docker-compose.yml` for Postgres, Kafka, and Kafka UI,
plus setup instructions.
