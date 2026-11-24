# Final assignment question: SmartDialer design

I would keep the pacing engine separate from the call-starting path. Progressive and
predictive strategies may calculate a requested number of calls, but neither strategy has
access to a telecom provider. A Safety Controller checks current available agents, ready
borrowers and provider health, then approves, reduces or rejects the request. The allocator
atomically reserves one agent and one borrower per approved call in a database transaction.

For a prototype, SQLite keeps the design easy to run locally while providing durable state,
transactional reservations and a persistent outbox. A unique active-call constraint ensures
one agent and one borrower cannot be allocated to two active calls. Provider callbacks go
through a persistent event inbox with event-ID de-duplication, sequence checks and a legal
state-transition map. Mock Provider A is fast and reliable; Mock Provider B adds delay,
timeouts, duplicate callbacks and out-of-order callbacks.

The predictive strategy estimates how many calls may be needed from answer rate, ringing
calls and average talk time. The Safety Controller still caps starts at the number of agents
that are actually free. This intentionally gives up aggressive over-dialling to preserve the
progressive dialer's deterministic safety property. If a provider fails repeatedly, a circuit
breaker rejects new calls. A worker restart resumes committed outbox work using the same call
ID; ambiguous calls that the provider cannot confirm remain reserved rather than being blindly
redialled.

For production, I would move the state store to PostgreSQL, use row-level locking and
`SKIP LOCKED`, add provider status reconciliation, worker lease renewal, compliance checks
(consent, do-not-call lists and local calling hours), monitoring, alerting and realistic
provider integration tests. I would not introduce Kafka or microservices until measured
throughput or operational needs justify them.
