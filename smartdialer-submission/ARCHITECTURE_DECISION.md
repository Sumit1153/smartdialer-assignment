# Architecture Decision Record

## Decision
Use a small Python application with SQLite, two provider adapters, two pacing strategies,
a dedicated Safety Controller, and a single transactional allocator/outbox.

## Why this design
The assignment's highest-risk issues are duplicate allocation, over-dialling, inconsistent
provider callbacks and worker crashes. A small architecture makes these controls visible and
runnable locally without requiring Redis, Kafka or multiple services.

## Trade-offs
- SQLite is easy to run and durable but a single-writer database; it is not the final choice
  for a very high-volume multi-region dialer.
- Predictive pacing is explainable and deliberately conservative. Safety caps it at live free
  agents, so the prototype gives up aggressive over-dialling to protect borrowers.
- Mock providers make tests repeatable but cannot model all real carrier behaviours.
- INITIATED/RINGING calls are kept reserved if provider status is unknown. This may reduce
  utilization, but avoids an unsafe duplicate redial.

## Production evolution
Move state to PostgreSQL, keep unique active-call constraints, add provider reconciliation,
worker lease renewal, persistent event inbox/outbox consumers, DNC/consent/time-zone enforcement,
metrics/alerts and load tests against realistic provider latency.
