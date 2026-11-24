import threading
import time
from smartdialer import Store, SafetyController, SmartDialer, ProviderEvent, predictive_pacing


def make_store(tmp_path, agents=2, borrowers=5):
    store = Store(str(tmp_path / "test.db"))
    for n in range(agents):
        store.add_agent(f"a{n}")
    for n in range(borrowers):
        store.add_borrower(f"b{n}", f"900000{n:04d}")
    return store


def test_safety_never_approves_more_than_available_agents(tmp_path):
    store = make_store(tmp_path, agents=2, borrowers=10)
    decision = SafetyController(store).approve(20)
    assert decision.approved == 2


def test_safety_also_checks_ready_borrowers(tmp_path):
    store = make_store(tmp_path, agents=5, borrowers=1)
    assert SafetyController(store).approve(5).approved == 1


def test_two_workers_cannot_reserve_same_agent(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=5)
    results = []
    lock = threading.Lock()
    def worker():
        value = store.reserve_calls(1, "test-provider")
        with lock:
            results.extend(value)
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert len(results) == 1
    assert store.available_agents() == 0


def test_duplicate_event_is_ignored(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a")[0]
    store.mark_initiated(call_id, "provider-call-1")
    event = ProviderEvent("provider-a", "event-1", call_id, "RINGING", 1, "provider-call-1")
    assert store.apply_event(event) == "APPLIED"
    assert store.apply_event(event) == "DUPLICATE_IGNORED"


def test_out_of_order_old_event_cannot_regress_call(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a")[0]
    store.mark_initiated(call_id, "provider-call-1")
    newer = ProviderEvent("provider-a", "event-2", call_id, "ANSWERED", 2, "provider-call-1")
    older = ProviderEvent("provider-a", "event-1", call_id, "RINGING", 1, "provider-call-1")
    assert store.apply_event(newer) == "APPLIED"
    assert store.apply_event(older) == "STALE_IGNORED"
    assert store.get_call(call_id)["state"] == "ANSWERED"


def test_expired_reserved_call_is_released_after_worker_crash(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a", lease_seconds=1)[0]
    recovered = store.recover_expired_reservations(time.time() + 2)
    assert recovered == 1
    assert store.get_call(call_id)["state"] == "FAILED"
    assert store.available_agents() == 1
    assert store.ready_borrowers() == 1


def test_connected_call_is_not_released_by_reservation_recovery(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a")[0]
    store.mark_initiated(call_id, "provider-call-1")
    event = ProviderEvent("provider-a", "event-2", call_id, "ANSWERED", 2, "provider-call-1")
    store.apply_event(event)
    store.recover_expired_reservations(time.time() + 1000)
    assert store.get_call(call_id)["state"] == "ANSWERED"
    assert store.available_agents() == 0


def test_predictive_plan_does_not_override_safety(tmp_path):
    store = make_store(tmp_path, agents=3, borrowers=10)
    plan = predictive_pacing(store, answer_rate=0.5, average_talk_seconds=120)
    decision = SafetyController(store).approve(plan.requested)
    assert plan.requested >= 3
    assert decision.approved <= 3


def test_provider_outage_blocks_new_calls(tmp_path):
    store = make_store(tmp_path, agents=2, borrowers=3)
    safety = SafetyController(store, failure_limit=2)
    safety.provider_failure()
    safety.provider_failure()
    assert safety.approve(2).approved == 0


def test_mock_provider_a_completes_call_lifecycle(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    dialer = SmartDialer(store, SafetyController(store), seed=1)
    # Use a provider with answer_rate=1 so the sequence is predictable.
    dialer.providers["A"].answer_rate = 1.0
    result = dialer.run_cycle("1", "A")
    call_id = result["call_ids"][0]
    assert store.get_call(call_id)["state"] == "COMPLETED"
    assert list(store.snapshot()["agents"].values()) == ["WRAP_UP"]


def test_worker_crash_after_answered_can_resume_with_completion_event(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a")[0]
    store.mark_initiated(call_id, "provider-call-1")
    answered = ProviderEvent("provider-a", "event-answered", call_id, "ANSWERED", 2, "provider-call-1")
    completed = ProviderEvent("provider-a", "event-completed", call_id, "COMPLETED", 4, "provider-call-1")
    assert store.apply_event(answered) == "APPLIED"

    # Simulate a restarted process opening the same SQLite database.
    restarted_store = Store(store.database)
    assert restarted_store.get_call(call_id)["state"] == "ANSWERED"
    assert restarted_store.apply_event(completed) == "APPLIED"
    assert restarted_store.get_call(call_id)["state"] == "COMPLETED"
    assert restarted_store.available_agents() == 0  # agent is in WRAP_UP, not silently re-used


def test_provider_failure_opens_circuit_and_blocks_remaining_dispatch(tmp_path):
    store = make_store(tmp_path, agents=4, borrowers=4)
    safety = SafetyController(store, failure_limit=1)
    dialer = SmartDialer(store, safety, seed=2)
    dialer.providers["B"].timeout_rate = 1.0
    result = dialer.run_cycle("1", "B")
    assert safety.provider_healthy is False
    assert result["metrics"]["initiated"] == 0
    assert store.available_agents() == 4


def test_two_workers_cannot_reserve_same_borrower(tmp_path):
    store = make_store(tmp_path, agents=5, borrowers=1)
    results = []
    lock = threading.Lock()
    def worker():
        value = store.reserve_calls(1, "test-provider")
        with lock:
            results.extend(value)
    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert len(results) == 1
    assert store.ready_borrowers() == 0


def test_outbox_resumes_reserved_call_after_worker_crash(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a")[0]
    assert len(store.pending_outbox()) == 1

    restarted_dialer = SmartDialer(store, SafetyController(store), seed=3)
    restarted_dialer.providers["A"].answer_rate = 1.0
    restarted_dialer.resume_pending_calls()

    assert store.get_call(call_id)["state"] == "COMPLETED"
    assert store.pending_outbox() == []


def test_outbox_resumes_after_provider_initiated_before_worker_crash(tmp_path):
    store = make_store(tmp_path, agents=1, borrowers=1)
    call_id = store.reserve_calls(1, "provider-a")[0]
    store.mark_initiated(call_id, f"A-{call_id}")  # worker crashed before processing callbacks
    restarted_dialer = SmartDialer(store, SafetyController(store), seed=4)
    restarted_dialer.providers["A"].answer_rate = 1.0
    restarted_dialer.resume_pending_calls()
    assert store.get_call(call_id)["state"] == "COMPLETED"
    assert store.pending_outbox() == []
