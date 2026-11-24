"""Basic concurrent allocation load test; no telecom calls are made."""
import argparse
import tempfile
import threading
import time
from pathlib import Path
from smartdialer import Store


def run(agents: int, workers: int):
    with tempfile.TemporaryDirectory() as folder:
        store = Store(str(Path(folder) / "load.db"))
        for n in range(agents):
            store.add_agent(f"agent-{n:06d}")
        for n in range(agents * 2):
            store.add_borrower(f"borrower-{n:06d}", f"900000{n:04d}")

        total = []
        total_lock = threading.Lock()
        def worker():
            while True:
                created = store.reserve_calls(1, "load-test-provider")
                if not created:
                    return
                with total_lock:
                    total.extend(created)

        start = time.perf_counter()
        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - start
        print({"agents": agents, "workers": workers, "reserved_calls": len(total),
               "seconds": round(elapsed, 3),
               "reservations_per_second": round(len(total) / elapsed, 1) if elapsed else 0})
        assert len(total) <= agents


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=int, default=100)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    run(args.agents, args.workers)
