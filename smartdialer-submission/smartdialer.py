"""A small, safety-first SmartDialer prototype using only the Python standard library.

This is a learning/submission prototype. Telecom providers are simulated; no real calls
are made. SQLite provides durable state and atomic reservations for local multi-worker tests.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import math
import random
import sqlite3
import time
import uuid
from contextlib import contextmanager


# -----------------------------------------------------------------------------
# 1. STATES: the allowed conditions for agents and calls
# -----------------------------------------------------------------------------
AGENT_STATES = ("OFFLINE", "AVAILABLE", "RESERVED", "DIALING", "CONNECTED", "WRAP_UP", "PAUSED")
CALL_STATES = ("QUEUED", "RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED",
               "COMPLETED", "FAILED", "CANCELLED")
ACTIVE_CALL_STATES = ("RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED")
TERMINAL_CALL_STATES = ("COMPLETED", "FAILED", "CANCELLED")


@dataclass(frozen=True)
class PacePlan:
    mode: str
    requested: int
    reason: str
    answer_rate: float = 0.0
    average_talk_seconds: float = 0.0


@dataclass(frozen=True)
class SafetyDecision:
    requested: int
    approved: int
    reason: str


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    event_id: str
    call_id: str
    event_type: str
    sequence: int
    provider_call_id: str


# -----------------------------------------------------------------------------
# 2. DATABASE: SQLite keeps agents, borrowers, calls and provider events
# -----------------------------------------------------------------------------
class Store:
    def __init__(self, database: str = "smartdialer.db"):
        self.database = str(database)
        if self.database != ":memory:":
            Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._create_tables()

    @contextmanager
    def connect(self):
        # Each operation gets its own connection. Always close it after use.
        connection = sqlite3.connect(self.database, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            yield connection
        finally:
            connection.close()

    def _create_tables(self):
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS agents (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    reserved_call_id TEXT,
                    lease_until REAL NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS borrowers (
                    id TEXT PRIMARY KEY,
                    phone TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'READY'
                );
                CREATE TABLE IF NOT EXISTS calls (
                    id TEXT PRIMARY KEY,
                    borrower_id TEXT NOT NULL REFERENCES borrowers(id),
                    agent_id TEXT NOT NULL REFERENCES agents(id),
                    provider TEXT NOT NULL,
                    provider_call_id TEXT,
                    state TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    lease_until REAL NOT NULL,
                    last_sequence INTEGER NOT NULL DEFAULT -1,
                    version INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_agent
                    ON calls(agent_id) WHERE state IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED');
                CREATE UNIQUE INDEX IF NOT EXISTS one_active_call_per_borrower
                    ON calls(borrower_id) WHERE state IN ('RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED');
                CREATE TABLE IF NOT EXISTS processed_events (
                    provider TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    processed_at REAL NOT NULL,
                    PRIMARY KEY(provider, event_id)
                );
                CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    dispatched INTEGER NOT NULL DEFAULT 0
                );
            """)

    def add_agent(self, agent_id: str, state: str = "AVAILABLE"):
        if state not in AGENT_STATES:
            raise ValueError(f"Unknown agent state: {state}")
        with self.connect() as db:
            db.execute("INSERT INTO agents(id,state) VALUES(?,?)", (agent_id, state))

    def add_borrower(self, borrower_id: str, phone: str, status: str = "READY"):
        with self.connect() as db:
            db.execute("INSERT INTO borrowers(id,phone,status) VALUES(?,?,?)",
                       (borrower_id, phone, status))

    def set_agent_state(self, agent_id: str, state: str):
        if state not in AGENT_STATES:
            raise ValueError(f"Unknown agent state: {state}")
        with self.connect() as db:
            db.execute("UPDATE agents SET state=?,version=version+1 WHERE id=?", (state, agent_id))

    def seed_demo_data(self, agents: int = 3, borrowers: int = 8):
        """Add sample rows only when the database is empty."""
        with self.connect() as db:
            count = db.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        if count == 0:
            for number in range(1, agents + 1):
                self.add_agent(f"agent-{number}")
        with self.connect() as db:
            count = db.execute("SELECT COUNT(*) FROM borrowers").fetchone()[0]
        if count == 0:
            for number in range(1, borrowers + 1):
                self.add_borrower(f"borrower-{number}", f"900000{number:04d}")

    def count(self, table: str, where: str = "1=1", values: tuple = ()) -> int:
        allowed_tables = {"agents", "borrowers", "calls"}
        if table not in allowed_tables:
            raise ValueError("Unsupported table")
        with self.connect() as db:
            return db.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", values).fetchone()[0]

    def available_agents(self) -> int:
        return self.count("agents", "state='AVAILABLE'")

    def ready_borrowers(self) -> int:
        return self.count("borrowers", "status='READY'")

    def ringing_calls(self) -> int:
        return self.count("calls", "state IN ('INITIATED','RINGING')")

    def connected_calls(self) -> int:
        return self.count("calls", "state IN ('ANSWERED','CONNECTED')")

    def reserve_calls(self, number: int, provider_name: str, lease_seconds: int = 45) -> list[str]:
        """Atomically reserve unique agent/borrower pairs and write outbox records.

        BEGIN IMMEDIATE serializes competing SQLite writers. Conditional updates and
        partial unique indexes are a second line of defence against double allocation.
        """
        if number <= 0:
            return []
        created_ids = []
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            agents = db.execute("SELECT id FROM agents WHERE state='AVAILABLE' ORDER BY id LIMIT ?",
                                (number,)).fetchall()
            borrowers = db.execute("SELECT id FROM borrowers WHERE status='READY' ORDER BY id LIMIT ?",
                                   (number,)).fetchall()
            for agent_row, borrower_row in zip(agents, borrowers):
                call_id = str(uuid.uuid4())
                agent_id, borrower_id = agent_row["id"], borrower_row["id"]
                agent_update = db.execute(
                    "UPDATE agents SET state='RESERVED', reserved_call_id=?, lease_until=?, version=version+1 "
                    "WHERE id=? AND state='AVAILABLE'", (call_id, now + lease_seconds, agent_id))
                borrower_update = db.execute(
                    "UPDATE borrowers SET status='RESERVED' WHERE id=? AND status='READY'", (borrower_id,))
                if agent_update.rowcount != 1 or borrower_update.rowcount != 1:
                    # Defensive rollback of a partial claim (normally prevented by the write lock).
                    db.execute("UPDATE agents SET state='AVAILABLE',reserved_call_id=NULL,lease_until=0 "
                               "WHERE id=? AND reserved_call_id=?", (agent_id, call_id))
                    db.execute("UPDATE borrowers SET status='READY' WHERE id=? AND status='RESERVED'",
                               (borrower_id,))
                    continue
                db.execute("""INSERT INTO calls
                    (id,borrower_id,agent_id,provider,state,created_at,updated_at,lease_until)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    (call_id, borrower_id, agent_id, provider_name, "RESERVED", now, now, now + lease_seconds))
                db.execute("INSERT INTO outbox(call_id,action,created_at) VALUES(?,?,?)",
                           (call_id, "START_CALL", now))
                created_ids.append(call_id)
            db.commit()
        return created_ids

    def get_call(self, call_id: str):
        with self.connect() as db:
            row = db.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()
            return dict(row) if row else None

    def get_phone_for_call(self, call_id: str) -> str:
        with self.connect() as db:
            row = db.execute("""SELECT b.phone FROM borrowers b
                JOIN calls c ON c.borrower_id=b.id WHERE c.id=?""", (call_id,)).fetchone()
            if not row:
                raise KeyError(call_id)
            return row["phone"]

    def pending_outbox(self) -> list[dict]:
        with self.connect() as db:
            rows = db.execute("""SELECT o.*,c.provider,c.state AS call_state
                FROM outbox o JOIN calls c ON c.id=o.call_id
                WHERE o.dispatched=0 ORDER BY o.id""").fetchall()
            return [dict(row) for row in rows]

    def mark_outbox_dispatched(self, outbox_id: int):
        with self.connect() as db:
            db.execute("UPDATE outbox SET dispatched=1 WHERE id=?", (outbox_id,))

    def mark_initiated(self, call_id: str, provider_call_id: str, lease_seconds: int = 90):
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT agent_id,state FROM calls WHERE id=?", (call_id,)).fetchone()
            if not row or row["state"] != "RESERVED":
                db.rollback()
                return False
            db.execute("UPDATE calls SET state='INITIATED',provider_call_id=?,updated_at=?,lease_until=?,version=version+1 WHERE id=?",
                       (provider_call_id, now, now + lease_seconds, call_id))
            db.execute("UPDATE agents SET state='DIALING',lease_until=?,version=version+1 WHERE id=?",
                       (now + lease_seconds, row["agent_id"]))
            db.commit()
            return True

    def fail_call(self, call_id: str, message: str):
        """Release resources after a definite setup/provider failure."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT agent_id,borrower_id,state FROM calls WHERE id=?", (call_id,)).fetchone()
            if not row or row["state"] in TERMINAL_CALL_STATES:
                db.rollback()
                return
            now = time.time()
            db.execute("UPDATE calls SET state='FAILED',error=?,updated_at=?,version=version+1 WHERE id=?",
                       (message, now, call_id))
            db.execute("UPDATE agents SET state='AVAILABLE',reserved_call_id=NULL,lease_until=0,version=version+1 WHERE id=?",
                       (row["agent_id"],))
            db.execute("UPDATE borrowers SET status='READY' WHERE id=?", (row["borrower_id"],))
            db.commit()

    def apply_event(self, event: ProviderEvent) -> str:
        """Deduplicate an event and apply only newer, legal state changes atomically."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            inserted = db.execute(
                "INSERT OR IGNORE INTO processed_events(provider,event_id,processed_at) VALUES(?,?,?)",
                (event.provider, event.event_id, time.time()))
            if inserted.rowcount == 0:
                db.rollback()
                return "DUPLICATE_IGNORED"
            row = db.execute("SELECT * FROM calls WHERE id=?", (event.call_id,)).fetchone()
            if not row:
                db.commit()
                return "UNKNOWN_CALL"
            current = row["state"]
            if current in TERMINAL_CALL_STATES or event.sequence <= row["last_sequence"]:
                db.commit()
                return "STALE_IGNORED"

            # The sequence guard handles old callbacks; this transition map also prevents regressions.
            allowed = {
                "RESERVED": {"RINGING", "ANSWERED", "CONNECTED", "COMPLETED", "FAILED"},
                "INITIATED": {"RINGING", "ANSWERED", "CONNECTED", "COMPLETED", "FAILED"},
                "RINGING": {"ANSWERED", "CONNECTED", "COMPLETED", "FAILED"},
                "ANSWERED": {"CONNECTED", "COMPLETED", "FAILED"},
                "CONNECTED": {"COMPLETED", "FAILED"},
            }
            if event.event_type not in allowed.get(current, set()):
                db.commit()
                return "INVALID_TRANSITION_IGNORED"

            now = time.time()
            db.execute("UPDATE calls SET state=?,last_sequence=?,updated_at=?,version=version+1 WHERE id=?",
                       (event.event_type, event.sequence, now, event.call_id))
            if event.event_type == "RINGING":
                db.execute("UPDATE agents SET state='DIALING',version=version+1 WHERE id=?", (row["agent_id"],))
            elif event.event_type in ("ANSWERED", "CONNECTED"):
                # The assigned agent is already reserved; move it to connected atomically.
                db.execute("UPDATE agents SET state='CONNECTED',version=version+1 WHERE id=?", (row["agent_id"],))
            elif event.event_type == "COMPLETED":
                db.execute("UPDATE agents SET state='WRAP_UP',reserved_call_id=NULL,lease_until=0,version=version+1 WHERE id=?",
                           (row["agent_id"],))
                db.execute("UPDATE borrowers SET status='CALLED' WHERE id=?", (row["borrower_id"],))
            elif event.event_type == "FAILED":
                db.execute("UPDATE agents SET state='AVAILABLE',reserved_call_id=NULL,lease_until=0,version=version+1 WHERE id=?",
                           (row["agent_id"],))
                db.execute("UPDATE borrowers SET status='READY' WHERE id=?", (row["borrower_id"],))
            db.commit()
            return "APPLIED"

    def finish_wrap_up(self, agent_id: str):
        with self.connect() as db:
            db.execute("UPDATE agents SET state='AVAILABLE',version=version+1 WHERE id=? AND state='WRAP_UP'",
                       (agent_id,))

    def recover_expired_reservations(self, now: Optional[float] = None) -> int:
        """Release only calls that never reached the provider (still RESERVED).

        An INITIATED/RINGING call is ambiguous after a worker crash. Keep its agent
        reserved until provider status is reconciled; never blindly redial it.
        """
        now = now or time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT id,agent_id,borrower_id FROM calls WHERE state='RESERVED' AND lease_until<?",
                              (now,)).fetchall()
            for row in rows:
                db.execute("UPDATE calls SET state='FAILED',error='reservation lease expired',updated_at=?,version=version+1 WHERE id=?",
                           (now, row["id"]))
                db.execute("UPDATE agents SET state='AVAILABLE',reserved_call_id=NULL,lease_until=0,version=version+1 WHERE id=?",
                           (row["agent_id"],))
                db.execute("UPDATE borrowers SET status='READY' WHERE id=?", (row["borrower_id"],))
            db.commit()
            return len(rows)

    def snapshot(self) -> dict:
        with self.connect() as db:
            result = {}
            for table, key, value in (("agents", "id", "state"), ("borrowers", "id", "status"), ("calls", "id", "state")):
                rows = db.execute(f"SELECT {key},{value} FROM {table} ORDER BY {key}").fetchall()
                result[table] = {row[key]: row[value] for row in rows}
            return result

    def close(self):
        # Connections are short-lived, so there is nothing persistent to close.
        pass


# -----------------------------------------------------------------------------
# 3. PACING: asks for a number of calls; it never starts calls itself
# -----------------------------------------------------------------------------
def progressive_pacing(store: Store) -> PacePlan:
    available = store.available_agents()
    return PacePlan("PROGRESSIVE", available,
                    f"One call per free agent: {available} agents are available.")


def predictive_pacing(store: Store, answer_rate: float = 0.50,
                      average_talk_seconds: float = 120,
                      setup_seconds: float = 3) -> PacePlan:
    """A small explainable estimator. Safety still clamps this request to real capacity."""
    if not 0 < answer_rate <= 1:
        raise ValueError("answer_rate must be greater than 0 and at most 1")
    if average_talk_seconds <= 0 or setup_seconds < 0:
        raise ValueError("Talk time must be positive and setup time cannot be negative")
    available = store.available_agents()
    ringing = store.ringing_calls()
    connected = store.connected_calls()
    # Longer conversations and ringing calls make the engine more cautious.
    talk_factor = min(1.0, 120.0 / average_talk_seconds)
    expected_free = max(0.0, available - ringing * answer_rate) * talk_factor
    estimated_slots = max(0, math.floor(expected_free))
    requested = math.ceil(estimated_slots / answer_rate) if estimated_slots else 0
    reason = (f"available={available}, ringing={ringing}, connected={connected}, "
              f"answer_rate={answer_rate:.0%}, average_talk={average_talk_seconds:.0f}s, "
              f"setup={setup_seconds:.0f}s, talk_factor={talk_factor:.2f}; "
              f"estimated spare slots={estimated_slots}.")
    return PacePlan("PREDICTIVE", requested, reason, answer_rate, average_talk_seconds)


# -----------------------------------------------------------------------------
# 4. SAFETY: the only component that approves a request
# -----------------------------------------------------------------------------
class SafetyController:
    def __init__(self, store: Store, failure_limit: int = 3):
        self.store = store
        self.failure_limit = failure_limit
        self.consecutive_provider_failures = 0
        self.provider_healthy = True

    def approve(self, requested: int) -> SafetyDecision:
        if not self.provider_healthy:
            return SafetyDecision(requested, 0, "Provider circuit is open; new calls are blocked.")
        available = self.store.available_agents()
        ready = self.store.ready_borrowers()
        approved = max(0, min(requested, available, ready))
        reason = ("Request is within live capacity." if approved == requested
                  else "Reduced to the live number of available agents and ready borrowers.")
        return SafetyDecision(requested, approved, reason)

    def provider_failure(self):
        self.consecutive_provider_failures += 1
        if self.consecutive_provider_failures >= self.failure_limit:
            self.provider_healthy = False

    def provider_success(self):
        self.consecutive_provider_failures = 0

    def provider_recovered(self):
        self.consecutive_provider_failures = 0
        self.provider_healthy = True


# -----------------------------------------------------------------------------
# 5. TWO MOCK PROVIDERS: same interface, different behaviour
# -----------------------------------------------------------------------------
class MockProvider:
    name = "base"

    def __init__(self, answer_rate: float = 0.5, seed: int = 7, latency_seconds: float = 0.0):
        self.answer_rate = answer_rate
        self.random = random.Random(seed)
        self.latency_seconds = latency_seconds
        self.saved_events: dict[str, list[ProviderEvent]] = {}

    def initiate(self, call_id: str, phone: str) -> str:
        raise NotImplementedError

    def get_events(self, call_id: str) -> list[ProviderEvent]:
        return list(self.saved_events.get(call_id, []))

    def make_event_stream(self, call_id: str, provider_call_id: str) -> list[ProviderEvent]:
        if call_id in self.saved_events:
            return list(self.saved_events[call_id])
        names = ["RINGING"]
        if self.random.random() < self.answer_rate:
            names += ["ANSWERED", "CONNECTED", "COMPLETED"]
        else:
            names += ["FAILED"]
        events = [ProviderEvent(self.name, f"{provider_call_id}-event-{i}", call_id, name, i, provider_call_id)
                  for i, name in enumerate(names, start=1)]
        self.saved_events[call_id] = events
        return events


class MockProviderA(MockProvider):
    name = "provider-a"

    def initiate(self, call_id: str, phone: str) -> str:
        print(f"Provider A: calling {phone}")
        if self.latency_seconds:
            time.sleep(self.latency_seconds)
        return f"A-{call_id}"  # fast, reliable, and idempotent for this call_id


class MockProviderB(MockProvider):
    name = "provider-b"

    def __init__(self, answer_rate: float = 0.5, seed: int = 19,
                 timeout_rate: float = 0.10, duplicate_rate: float = 0.30,
                 reorder_rate: float = 0.30):
        super().__init__(answer_rate, seed, latency_seconds=0.02)
        self.timeout_rate = timeout_rate
        self.duplicate_rate = duplicate_rate
        self.reorder_rate = reorder_rate

    def initiate(self, call_id: str, phone: str) -> str:
        print(f"Provider B: calling {phone}")
        if self.latency_seconds:
            time.sleep(self.latency_seconds)
        if self.random.random() < self.timeout_rate:
            raise TimeoutError("Provider B timed out before confirming call creation (mock behaviour).")
        return f"B-{call_id}"

    def make_event_stream(self, call_id: str, provider_call_id: str) -> list[ProviderEvent]:
        events = super().make_event_stream(call_id, provider_call_id)
        if events and self.random.random() < self.duplicate_rate:
            events.append(events[-1])  # identical event ID simulates duplicate delivery
        if len(events) > 1 and self.random.random() < self.reorder_rate:
            events = list(reversed(events))  # sequence numbers still show original order
        return events


# -----------------------------------------------------------------------------
# 6. DIALER: combines pacing, safety, allocation and provider callbacks
# -----------------------------------------------------------------------------
class SmartDialer:
    def __init__(self, store: Store, safety: SafetyController, seed: int = 7):
        self.store = store
        self.safety = safety
        self.providers = {
            "A": MockProviderA(answer_rate=0.50, seed=seed),
            "B": MockProviderB(answer_rate=0.50, seed=seed + 1),
        }
        self.metrics = {"initiated": 0, "connected": 0, "failed": 0,
                        "events_applied": 0, "duplicate_or_stale_events": 0}

    def run_cycle(self, mode: str, provider_letter: str = "A", answer_rate: float = 0.50,
                  average_talk_seconds: float = 120) -> dict:
        provider_letter = provider_letter.upper()
        if provider_letter not in self.providers:
            raise ValueError("Provider must be A or B")
        provider = self.providers[provider_letter]
        # Recover committed start-call jobs before calculating fresh capacity.
        self.resume_pending_calls()
        if mode == "1":
            plan = progressive_pacing(self.store)
        elif mode == "2":
            plan = predictive_pacing(self.store, answer_rate, average_talk_seconds)
        else:
            raise ValueError("Mode must be 1 (Progressive) or 2 (Predictive)")

        decision = self.safety.approve(plan.requested)
        self.metrics["safety_approved"] = self.metrics.get("safety_approved", 0) + decision.approved
        self.metrics["safety_rejected"] = self.metrics.get("safety_rejected", 0) + max(0, decision.requested - decision.approved)
        print("\n--- PACING ENGINE ---")
        print(f"Mode: {plan.mode}; calls requested: {plan.requested}")
        print("Reason:", plan.reason)
        print("\n--- SAFETY CONTROLLER ---")
        print(f"Requested={decision.requested}; approved={decision.approved}")
        print("Decision:", decision.reason)

        # Only the approved count reaches the allocator.
        call_ids = self.store.reserve_calls(decision.approved, provider.name)
        for call_id in call_ids:
            self._dispatch(call_id, provider)
        snapshot = self.store.snapshot()
        total_agents = self.store.count("agents")
        occupied_or_wrap_up = self.store.count("agents", "state IN ('RESERVED','DIALING','CONNECTED','WRAP_UP')")
        utilization_proxy = round(100 * occupied_or_wrap_up / total_agents, 1) if total_agents else 0.0
        return {"plan": plan, "decision": decision, "call_ids": call_ids,
                "metrics": dict(self.metrics), "snapshot": snapshot,
                "agent_utilization_proxy_pct": utilization_proxy}

    def _dispatch(self, call_id: str, provider: MockProvider):
        outbox_rows = [row for row in self.store.pending_outbox() if row["call_id"] == call_id]
        if not outbox_rows:
            return
        outbox_id = outbox_rows[0]["id"]
        call = self.store.get_call(call_id)
        if not call or call["state"] in TERMINAL_CALL_STATES:
            self.store.mark_outbox_dispatched(outbox_id)
            return
        # A circuit can open while a reserved batch is being dispatched. Stop immediately.
        if not self.safety.provider_healthy:
            self.store.fail_call(call_id, "provider circuit opened before this call was dispatched")
            self.store.mark_outbox_dispatched(outbox_id)
            return
        phone = self.store.get_phone_for_call(call_id)
        try:
            if call["state"] == "RESERVED":
                # The first provider request uses our call ID as its idempotency key.
                provider_call_id = provider.initiate(call_id, phone)
                if not self.store.mark_initiated(call_id, provider_call_id):
                    return
                self.metrics["initiated"] += 1
                self.safety.provider_success()
            else:
                # The DB already says the provider accepted this call. Never create it again.
                provider_call_id = call.get("provider_call_id")
                if not provider_call_id:
                    # Unknown provider outcome: leave the outbox pending for manual reconciliation.
                    return
            # Keep the outbox open until callbacks are processed. If a worker crashes midway,
            # retrying replays the same event IDs and the event inbox safely ignores duplicates.
            for event in provider.make_event_stream(call_id, provider_call_id):
                result = self.store.apply_event(event)
                if result == "APPLIED":
                    self.metrics["events_applied"] += 1
                    if event.event_type == "ANSWERED":
                        self.metrics["connected"] += 1
                    if event.event_type == "FAILED":
                        self.metrics["failed"] += 1
                else:
                    self.metrics["duplicate_or_stale_events"] += 1
            self.store.mark_outbox_dispatched(outbox_id)
        except TimeoutError as exc:
            # In this mock, timeout means the provider did not create the call. Real provider
            # timeouts can be ambiguous and must be reconciled before retrying.
            self.metrics["failed"] += 1
            self.safety.provider_failure()
            self.store.fail_call(call_id, str(exc))
            self.store.mark_outbox_dispatched(outbox_id)

    def resume_pending_calls(self):
        """Process committed outbox work left behind by a crashed worker."""
        pending = self.store.pending_outbox()
        for row in pending:
            if row["provider"] == "provider-a":
                provider = self.providers["A"]
            elif row["provider"] == "provider-b":
                provider = self.providers["B"]
            else:
                # Unknown provider adapter: leave the outbox row pending for operator action.
                continue
            self._dispatch(row["call_id"], provider)

    def recover(self, now: Optional[float] = None) -> int:
        """Release reservations that expired before provider initiation."""
        return self.store.recover_expired_reservations(now)

    def reconcile_initiated_calls(self, provider_letter: str = "A") -> int:
        """Replay known mock-provider callbacks after a worker restart.

        Unknown/ambiguous provider calls stay reserved; this method never redials them.
        """
        provider = self.providers[provider_letter.upper()]
        with self.store.connect() as db:
            rows = db.execute("SELECT id FROM calls WHERE provider=? AND state IN ('INITIATED','RINGING')",
                              (provider.name,)).fetchall()
        recovered = 0
        for row in rows:
            events = provider.get_events(row["id"])
            for event in events:
                result = self.store.apply_event(event)
                if result == "APPLIED":
                    recovered += 1
        return recovered

    def finish_wrap_up_agents(self):
        snap = self.store.snapshot()["agents"]
        for agent_id, state in snap.items():
            if state == "WRAP_UP":
                self.store.finish_wrap_up(agent_id)


def run_simulation_scenarios():
    """Run four pacing profiles in separate temporary SQLite databases."""
    import tempfile
    scenarios = [("A", 0.20, 120), ("B", 0.50, 90), ("C", 0.70, 180), ("D", 0.10, 240)]
    for name, answer_rate, talk_time in scenarios:
        with tempfile.TemporaryDirectory() as folder:
            store = Store(str(Path(folder) / "scenario.db"))
            store.seed_demo_data(agents=10, borrowers=40)
            dialer = SmartDialer(store, SafetyController(store), seed=10 + ord(name))
            result = dialer.run_cycle("2", "A", answer_rate, talk_time)
            print(f"\nScenario {name}: answer={answer_rate:.0%}, average talk={talk_time}s")
            print("Initiated:", result["metrics"]["initiated"],
                  "Connected:", result["metrics"]["connected"],
                  "Failed:", result["metrics"]["failed"])
            print("Agent utilization proxy:", result["agent_utilization_proxy_pct"], "%")
            print("Safety approved/rejected:", result["metrics"]["safety_approved"],
                  "/", result["metrics"]["safety_rejected"])
            print("Agent states:", result["snapshot"]["agents"])


def run_failure_demos():
    """Show small deterministic examples for the assignment's named failure cases."""
    import tempfile
    print("\n=== WORKER CRASH: reservation expired before dialing ===")
    with tempfile.TemporaryDirectory() as folder:
        store = Store(str(Path(folder) / "crash.db"))
        store.add_agent("agent-1")
        store.add_borrower("borrower-1", "9000000001")
        call_id = store.reserve_calls(1, "provider-a", lease_seconds=1)[0]
        released = store.recover_expired_reservations(time.time() + 2)
        print("Recovered reservations:", released, "Call state:", store.get_call(call_id)["state"])

    print("\n=== PROVIDER OUTAGE: circuit breaker blocks new calls ===")
    with tempfile.TemporaryDirectory() as folder:
        store = Store(str(Path(folder) / "outage.db"))
        store.add_agent("agent-1")
        store.add_borrower("borrower-1", "9000000001")
        safety = SafetyController(store, failure_limit=3)
        for _ in range(3):
            safety.provider_failure()
        print("Provider healthy:", safety.provider_healthy, "New calls approved:", safety.approve(1).approved)

    print("\n=== AGENT AVAILABILITY DROP: safety uses live availability ===")
    with tempfile.TemporaryDirectory() as folder:
        store = Store(str(Path(folder) / "agents.db"))
        for n in range(5):
            store.add_agent(f"agent-{n}")
        for n in range(5):
            store.add_borrower(f"borrower-{n}", f"900000{n:04d}")
        for n in range(3):
            store.set_agent_state(f"agent-{n}", "OFFLINE")
        print("Available agents:", store.available_agents(), "Calls approved:", SafetyController(store).approve(5).approved)

    print("\n=== DUPLICATE AND OUT-OF-ORDER EVENTS ===")
    with tempfile.TemporaryDirectory() as folder:
        store = Store(str(Path(folder) / "events.db"))
        store.add_agent("agent-1")
        store.add_borrower("borrower-1", "9000000001")
        call_id = store.reserve_calls(1, "provider-a")[0]
        store.mark_initiated(call_id, "provider-call-1")
        new_event = ProviderEvent("provider-a", "event-2", call_id, "ANSWERED", 2, "provider-call-1")
        old_event = ProviderEvent("provider-a", "event-1", call_id, "RINGING", 1, "provider-call-1")
        print("First delivery:", store.apply_event(new_event))
        print("Duplicate delivery:", store.apply_event(new_event))
        print("Late older event:", store.apply_event(old_event))
        print("Final call state:", store.get_call(call_id)["state"])


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Safety-first SmartDialer demo (mock calls only)")
    parser.add_argument("--db", default="smartdialer.db", help="SQLite database file")
    parser.add_argument("--reset", action="store_true", help="Delete and recreate the demo database")
    parser.add_argument("--scenarios", action="store_true", help="Run four predictive pacing scenarios")
    parser.add_argument("--failures", action="store_true", help="Demonstrate worker/provider/event failure cases")
    args = parser.parse_args()

    if args.scenarios:
        run_simulation_scenarios()
        return
    if args.failures:
        run_failure_demos()
        return
    if args.reset and args.db != ":memory:":
        Path(args.db).unlink(missing_ok=True)

    store = Store(args.db)
    store.seed_demo_data(agents=3, borrowers=8)
    print("===== BEGINNER SMARTDIALER =====")
    print("This is a simulation; it does not make real phone calls.")
    print("1. Progressive Mode")
    print("2. Predictive Mode")
    mode = input("Choose mode (1 or 2): ").strip()
    provider = input("Choose provider (A or B): ").strip().upper() or "A"
    dialer = SmartDialer(store, SafetyController(store))
    try:
        dialer.run_cycle(mode, provider)
    except ValueError as exc:
        print("Input error:", exc)
        return
    print("\n--- FINAL SUMMARY ---")
    for group, values in store.snapshot().items():
        print(group.upper() + ":", values)
    print("METRICS:", dialer.metrics)
    print("\nAgents in WRAP_UP must be marked AVAILABLE after the agent finishes wrap-up.")
    answer = input("Mark all WRAP_UP agents AVAILABLE now? (y/n): ").strip().lower()
    if answer == "y":
        dialer.finish_wrap_up_agents()
        print("Wrap-up completed. Available agents:", store.available_agents())


if __name__ == "__main__":
    main()
