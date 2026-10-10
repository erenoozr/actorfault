"""ActorFault — a fault-tolerant actor system with a live deadlock detector.

Run:  python actorfault.py   (Python 3.10+, standard library only)
"""
from __future__ import annotations

import logging
import queue
import random
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

log = logging.getLogger("actorfault")
_ctx = threading.local()  # actor currently handling a message; tells ask() who is waiting


# ══════════════════════════════════════════════════════════════════════
# STAGE 1: Actor lifecycle and isolated mailboxes
# ══════════════════════════════════════════════════════════════════════

class ActorState(Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    CRASHED = "CRASHED"
    STOPPED = "STOPPED"


class ActorStopped(Exception):
    """A pending ask was cancelled because the actor is stopping."""


class DeadLetter(Exception):
    """The target actor is permanently dead (its supervisor gave up); the message could not be delivered."""


class Future:
    """One-shot reply slot; the first set_* wins (a stop cancellation may race a real reply)."""

    def __init__(self) -> None:
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._value: Any = None
        self._error: BaseException | None = None

    def _settle(self, value: Any, error: BaseException | None) -> None:
        with self._lock:
            if self._done.is_set():
                return
            self._value, self._error = value, error
            self._done.set()

    def set_result(self, value: Any) -> None:
        self._settle(value, None)

    def set_exception(self, error: BaseException) -> None:
        self._settle(None, error)

    def result(self, timeout: float) -> Any:
        if not self._done.wait(timeout):
            raise TimeoutError(f"no reply within {timeout}s")
        if self._error is not None:
            raise self._error
        return self._value


@dataclass
class Envelope:
    message: Any
    sender: str | None = None
    future: Future | None = None  # set only for ask messages


class Actor:
    """Each instance runs on its own thread; the mailbox belongs to the ActorSystem and survives restarts."""

    def __init__(self) -> None:
        self.id = f"{type(self).__name__}-{uuid.uuid4().hex[:6]}"
        self.state = ActorState.STARTING
        self.system: ActorSystem | None = None
        self.supervisor_id: str | None = None
        self.mailbox: queue.Queue | None = None
        self._halt = threading.Event()
        self._waiting: Future | None = None
        self._thread: threading.Thread | None = None

    # ── hooks for subclasses ──
    def on_start(self) -> None: ...
    def on_stop(self) -> None: ...

    def receive(self, message: Any, sender: str | None) -> Any:
        raise NotImplementedError

    # ── messaging ──
    def send(self, target_actor_id: str, message: Any) -> None:
        self.system.send(target_actor_id, message, sender=self.id)

    def ask(self, target_actor_id: str, message: Any, timeout: float = 2.0) -> Any:
        return self.system.ask(target_actor_id, message, timeout)

    # ── lifecycle ──
    def stop(self, timeout: float = 2.0) -> None:
        self._halt.set()
        waiting = self._waiting  # read AFTER the flag; ask() does the reverse, so one always sees the other
        if waiting is not None:
            waiting.set_exception(ActorStopped(f"{self.id} is stopping"))
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout)
            if self._thread.is_alive():
                log.warning("%s did not stop within %ss", self.id, timeout)

    def _run(self) -> None:
        _ctx.actor = self
        try:
            self.on_start()
        except Exception as exc:
            self._crash(exc)
            return
        self.state = ActorState.RUNNING
        while not self._halt.is_set():
            try:
                env = self.mailbox.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                reply = self.receive(env.message, env.sender)
            except Exception as exc:
                if env.future is not None:
                    env.future.set_exception(exc)  # the asker sees the error instead of hanging
                if self._halt.is_set():
                    break  # stop() cancelled a pending ask: not a crash
                self._crash(exc)
                return
            if env.future is not None:
                env.future.set_result(reply)
        try:
            self.on_stop()
        finally:
            self.state = ActorState.STOPPED

    def _crash(self, exc: Exception) -> None:
        self.state = ActorState.CRASHED
        log.error("%s CRASHED: %s: %s", self.id, type(exc).__name__, exc)
        self.system._report_crash(self, exc)


class ActorSystem:
    def __init__(self) -> None:
        self.wait_graph = WaitGraph()
        self.escalations: list[tuple[str, BaseException]] = []
        self.dead_letters: list[tuple[str, Envelope]] = []
        self._lock = threading.Lock()
        self._retired: set[str] = set()
        self._actors: dict[str, Actor] = {}
        self._mailboxes: dict[str, queue.Queue] = {}

    def spawn(self, actor_id: str, factory: Callable[[], Actor], supervisor_id: str | None = None) -> Actor:
        actor = factory()
        actor.id, actor.system, actor.supervisor_id = actor_id, self, supervisor_id
        with self._lock:
            # the mailbox outlives the actor instance: queued messages survive a restart
            actor.mailbox = self._mailboxes.setdefault(actor_id, queue.Queue())
            self._actors[actor_id] = actor
            self._retired.discard(actor_id)
        actor._thread = threading.Thread(target=actor._run, name=actor_id, daemon=True)
        actor._thread.start()
        return actor

    def get(self, actor_id: str) -> Actor | None:
        with self._lock:
            return self._actors.get(actor_id)

    def _deliver(self, target: str, env: Envelope) -> None:
        with self._lock:
            mailbox = self._mailboxes.get(target)
            if mailbox is None:
                raise KeyError(f"unknown actor: {target}")
            if target not in self._retired:
                mailbox.put(env)  # under the lock: cannot race _retire and land in an already drained queue
                return
        self._dead_letter(target, env)

    def _retire(self, actor_id: str) -> None:
        """Mark the actor permanently dead; queued and later messages become dead letters (spawn undoes this)."""
        with self._lock:
            self._retired.add(actor_id)
            mailbox = self._mailboxes.get(actor_id)
        while mailbox is not None:
            try:
                env = mailbox.get_nowait()
            except queue.Empty:
                break
            self._dead_letter(actor_id, env)

    def _dead_letter(self, target: str, env: Envelope) -> None:
        self.dead_letters.append((target, env))
        log.warning("DEAD LETTER → %s: %r", target, env.message)
        if env.future is not None:
            env.future.set_exception(DeadLetter(f"{target} is permanently dead"))  # ask fails fast instead of timing out

    def send(self, target_actor_id: str, message: Any, sender: str | None = None) -> None:
        self._deliver(target_actor_id, Envelope(message, sender))

    def ask(self, target_actor_id: str, message: Any, timeout: float = 2.0) -> Any:
        caller: Actor | None = getattr(_ctx, "actor", None)
        future = Future()
        if caller is not None:
            self.wait_graph.add_wait(caller.id, target_actor_id)  # raises here if this wait would close a cycle
            caller._waiting = future
        try:
            if caller is not None and caller._halt.is_set():
                raise ActorStopped(f"{caller.id} is stopping")
            self._deliver(target_actor_id, Envelope(message, caller.id if caller else None, future))
            return future.result(timeout)
        finally:
            if caller is not None:
                caller._waiting = None
                self.wait_graph.remove_wait(caller.id, target_actor_id)

    def _report_crash(self, actor: Actor, exc: Exception) -> None:
        if actor.supervisor_id:
            self.send(actor.supervisor_id, ChildCrashed(actor.id, actor, exc))
        else:
            log.critical("ESCALATION: unsupervised %s crashed, reported to system level: %s", actor.id, exc)
            self.escalations.append((actor.id, exc))
            self._retire(actor.id)

    def shutdown(self) -> None:
        with self._lock:
            actors = list(self._actors.values())
        for actor in reversed(actors):
            actor.stop()
        for actor in actors:
            self._retire(actor.id)


# ══════════════════════════════════════════════════════════════════════
# STAGE 2: Supervision tree and restart strategies
# ══════════════════════════════════════════════════════════════════════

class Strategy(Enum):
    ONE_FOR_ONE = "OneForOne"
    ALL_FOR_ONE = "AllForOne"


@dataclass(frozen=True)
class ChildSpec:
    actor_id: str
    factory: Callable[[], Actor]  # builds a fresh instance on every restart


@dataclass
class ChildCrashed:
    actor_id: str
    instance: Actor
    error: BaseException


class SupervisorGaveUp(Exception):
    """Restart intensity exceeded; the supervisor crashes itself and escalates to its parent."""


class Supervisor(Actor):
    """An actor itself: crash notices arrive in its mailbox; when it gives up, its own supervisor handles it."""

    def __init__(self, children: list[ChildSpec], strategy: Strategy = Strategy.ONE_FOR_ONE,
                 max_restarts: int = 3, within: float = 5.0) -> None:
        super().__init__()
        self.children = list(children)
        self.strategy = strategy
        self.max_restarts = max_restarts
        self.within = within
        self.restart_count = 0
        self._crash_times: list[float] = []

    def on_start(self) -> None:
        for spec in self.children:
            self.system.spawn(spec.actor_id, spec.factory, supervisor_id=self.id)

    def on_stop(self) -> None:
        self._stop_children()

    def _stop_children(self) -> None:
        for spec in reversed(self.children):
            child = self.system.get(spec.actor_id)
            if child is not None:
                child.stop()

    def receive(self, message: Any, sender: str | None) -> Any:
        if not isinstance(message, ChildCrashed):
            raise TypeError(f"{self.id} unexpected message: {message!r}")
        if self.system.get(message.actor_id) is not message.instance:
            return None  # instance already replaced (e.g. the AllForOne group was restarted together)

        now = time.monotonic()
        self._crash_times = [t for t in self._crash_times if now - t <= self.within] + [now]
        if len(self._crash_times) > self.max_restarts:
            log.critical("%s GAVE UP: %d crashes in the last %.0fs (limit %d) → escalating",
                         self.id, len(self._crash_times), self.within, self.max_restarts)
            self._stop_children()
            for spec in self.children:
                self.system._retire(spec.actor_id)
            raise SupervisorGaveUp(f"{self.id}: {message.actor_id} crashes too often ({message.error!r})")

        if self.strategy is Strategy.ALL_FOR_ONE:
            group = self.children
        else:
            group = [s for s in self.children if s.actor_id == message.actor_id]
        for spec in reversed(group):
            child = self.system.get(spec.actor_id)
            if child is not None:
                child.stop()
        self.restart_count += 1  # before spawn: every reply from the new instance happens after this
        for spec in group:
            pending = self.system.get(spec.actor_id).mailbox.qsize()
            self.system.spawn(spec.actor_id, spec.factory, supervisor_id=self.id)
            log.warning("%s [%s] restarted %s (queued messages: %d)",
                        self.id, self.strategy.value, spec.actor_id, pending)
        return None


# ══════════════════════════════════════════════════════════════════════
# STAGE 3: Wait-for graph and deadlock detector
# ══════════════════════════════════════════════════════════════════════

class DeadlockDetectedException(Exception):
    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        super().__init__("deadlock cycle: " + " -> ".join(cycle))


class WaitGraph:
    """Edge A -> B: A is waiting for B's ask reply. Cycles are checked before an edge is added."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._edges: dict[str, set[str]] = {}

    def add_wait(self, waiter: str, target: str) -> None:
        # Check + insert under one lock: of concurrent A->B and B->A, one always sees the cycle.
        with self._lock:
            path = self._path(target, waiter)
            if path is not None:
                cycle = [waiter, *path]
                log.critical("DEADLOCK DETECTED: %s (edge not added, ask aborted)", " -> ".join(cycle))
                raise DeadlockDetectedException(cycle)
            self._edges.setdefault(waiter, set()).add(target)

    def remove_wait(self, waiter: str, target: str) -> None:
        with self._lock:
            targets = self._edges.get(waiter)
            if targets:
                targets.discard(target)
                if not targets:
                    del self._edges[waiter]

    def snapshot(self) -> dict[str, list[str]]:
        with self._lock:
            return {k: sorted(v) for k, v in self._edges.items()}

    def _path(self, src: str, dst: str) -> list[str] | None:
        """Path from src to dst (iterative DFS). A new edge waiter->target closes a cycle only if target reaches waiter."""
        stack = [(src, [src])]
        seen: set[str] = set()
        while stack:
            node, path = stack.pop()
            if node == dst:
                return path
            if node in seen:
                continue
            seen.add(node)
            for nxt in self._edges.get(node, ()):
                stack.append((nxt, path + [nxt]))
        return None


# ══════════════════════════════════════════════════════════════════════
# STAGE 4: Distributed counter and cache actors
# ══════════════════════════════════════════════════════════════════════

class DatabaseActor(Actor):
    """storage and rng come from the factory closure: the 'disk' survives restarts, the connection is reset."""

    def __init__(self, storage: dict, rng: random.Random, failure_rate: float = 0.0, latency: float = 0.0) -> None:
        super().__init__()
        self.storage, self.rng, self.failure_rate, self.latency = storage, rng, failure_rate, latency

    def on_start(self) -> None:
        log.info("%s connection opened (records: %d)", self.id, len(self.storage))

    def receive(self, message: Any, sender: str | None) -> Any:
        op, *args = message
        if op == "crash":
            raise RuntimeError("intentional crash")
        time.sleep(self.latency)
        # Raised BEFORE any mutation, so retrying after a ConnectionError is safe.
        if self.rng.random() < self.failure_rate:
            raise ConnectionError("transient connection failure")
        if op == "get":
            return self.storage.get(args[0], 0)
        if op == "incr":
            key, amount = args
            self.storage[key] = self.storage.get(key, 0) + amount
            return self.storage[key]
        raise ValueError(f"unknown operation: {op}")


class CacheActor(Actor):
    def __init__(self, db_id: str, retries: int = 5) -> None:
        super().__init__()
        self.db_id, self.retries = db_id, retries
        self.cache: dict[str, Any] = {}
        self.hits = 0

    def receive(self, message: Any, sender: str | None) -> Any:
        op, key, *_ = message
        if op == "get" and key in self.cache:
            self.hits += 1
            return self.cache[key]
        value = self._db(message)
        self.cache[key] = value  # write-through for incr
        return value

    def _db(self, message: Any) -> Any:
        for attempt in range(1, self.retries + 1):
            try:
                return self.ask(self.db_id, message, timeout=2.0)
            except ConnectionError as exc:
                # Retry only ConnectionError; after a TimeoutError the incr may already be applied.
                log.info("%s → %s attempt %d failed (%s), retrying", self.id, self.db_id, attempt, exc)
                time.sleep(0.02 * attempt)
        raise ConnectionError(f"{self.db_id} did not respond after {self.retries} attempts")


class WorkerActor(Actor):
    def __init__(self, cache_id: str) -> None:
        super().__init__()
        self.cache_id = cache_id

    def receive(self, message: Any, sender: str | None) -> Any:
        return self.ask(self.cache_id, message, timeout=5.0)


class CounterActor(Actor):
    """Simple actor with in-memory state (for the AllForOne and escalation demos)."""

    def __init__(self) -> None:
        super().__init__()
        self.count = 0

    def receive(self, message: Any, sender: str | None) -> Any:
        if message == "crash":
            raise RuntimeError("intentional crash")
        if message == "incr":
            self.count += 1
        return self.count


class PeerActor(Actor):
    """("relay", [b, c, ...]): forwards to the next actor with a synchronous ask; replies 'pong' at the end."""

    def receive(self, message: Any, sender: str | None) -> Any:
        _, route = message
        if not route:
            return f"pong@{self.id}"
        return self.ask(route[0], ("relay", route[1:]), timeout=3.0)


# ══════════════════════════════════════════════════════════════════════
# STAGE 5: Chaos test and live verification
# ══════════════════════════════════════════════════════════════════════

def banner(title: str) -> None:
    print(f"\n{'═' * 70}\n  {title}\n{'═' * 70}", flush=True)


def wait_until(cond: Callable[[], bool], timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def chaos_db_crash() -> None:
    banner("CHAOS 1: DatabaseActor crashed on purpose, queued messages are preserved")
    system, storage = ActorSystem(), {}
    db_spec = ChildSpec("db", lambda: DatabaseActor(storage, random.Random(0)))
    system.spawn("db-sup", lambda: Supervisor([db_spec]))
    assert wait_until(lambda: system.get("db") is not None)
    first = system.get("db")

    system.send("db", ("crash",))
    for _ in range(5):
        system.send("db", ("incr", "x", 1))  # these wait in the queue behind the crash
    value = system.ask("db", ("get", "x"), timeout=3.0)

    print(f"  → after crash x = {value}  (expected 5)  | new instance: {system.get('db') is not first}")
    assert value == 5 and system.get("db") is not first
    assert system.get("db-sup").restart_count == 1
    system.shutdown()


def chaos_load_with_random_failures() -> None:
    banner("CHAOS 2: Worker → Cache → DB under load with random ConnectionErrors")
    system, storage = ActorSystem(), {}
    rng = random.Random(4)  # deterministic since the DB draws sequentially: exactly 3 failures in 30 ops (at the limit)
    workers = [f"worker-{i}" for i in range(1, 4)]
    specs = [ChildSpec("db", lambda: DatabaseActor(storage, rng, failure_rate=0.1, latency=0.003)),
             ChildSpec("cache", lambda: CacheActor("db"))]
    specs += [ChildSpec(w, lambda: WorkerActor("cache")) for w in workers]
    system.spawn("root", lambda: Supervisor(specs))
    assert wait_until(lambda: all(system.get(w) for w in workers))

    per_client, errors = 10, []

    def client(worker_id: str) -> None:
        for _ in range(per_client):
            try:
                system.ask(worker_id, ("incr", "hits", 1), timeout=10.0)
            except Exception as exc:  # clients must never see an error
                errors.append(exc)

    threads = [threading.Thread(target=client, args=(w,), name=f"client-{w}") for w in workers]
    for t in threads:
        t.start()
    time.sleep(0.03)
    print(f"  → live wait-for graph: {system.wait_graph.snapshot()}")
    for t in threads:
        t.join()

    total = system.ask("worker-1", ("get", "hits"), timeout=5.0)
    restarts = system.get("root").restart_count
    print(f"  → counter = {total} (expected {per_client * len(workers)}), client errors = {len(errors)}, "
          f"DB restarts = {restarts}")
    assert not errors and total == per_client * len(workers) and restarts >= 1
    system.shutdown()


def chaos_all_for_one() -> None:
    banner("CHAOS 3: AllForOne — one sibling crashes, the whole group is reset")
    system = ActorSystem()
    system.spawn("group-sup", lambda: Supervisor(
        [ChildSpec("a", CounterActor), ChildSpec("b", CounterActor)], strategy=Strategy.ALL_FOR_ONE))
    assert wait_until(lambda: system.get("b") is not None)
    for _ in range(3):
        system.ask("b", "incr")
    print(f"  → before crash b = {system.ask('b', 'get')}")
    system.send("a", "crash")
    assert wait_until(lambda: system.get("group-sup").restart_count == 1)
    b_after = system.ask("b", "get")
    print(f"  → a crashed, b was restarted too: b = {b_after}  (expected 0)")
    assert b_after == 0
    system.shutdown()


def chaos_escalation() -> None:
    banner("CHAOS 4: Restart limit — more than 3 crashes in 5 s, supervisor gives up and escalates")
    system = ActorSystem()
    flaky_sup = ChildSpec("flaky-sup", lambda: Supervisor([ChildSpec("flaky", CounterActor)]))
    system.spawn("root", lambda: Supervisor([flaky_sup]))
    assert wait_until(lambda: system.get("flaky") is not None)
    first_sup = system.get("flaky-sup")
    for _ in range(4):
        system.send("flaky", "crash")
    assert wait_until(lambda: system.get("root").restart_count == 1)
    assert system.get("flaky-sup") is not first_sup
    alive = system.ask("flaky", "incr", timeout=3.0)
    print(f"  → root rebuilt flaky-sup (and its subtree); flaky responds: count = {alive}")
    assert alive == 1
    system.shutdown()


def chaos_deadlock() -> None:
    banner("CHAOS 5: Intentional circular dependency — the detector prevents the deadlock")
    system = ActorSystem()
    peers = [ChildSpec(p, PeerActor) for p in ("alice", "bob", "carol")]
    system.spawn("peer-sup", lambda: Supervisor(peers, max_restarts=10))
    assert wait_until(lambda: system.get("carol") is not None)

    ok = system.ask("alice", ("relay", ["bob", "carol"]), timeout=3.0)
    print(f"  → acyclic chain alice→bob→carol: {ok}  (no false positive)")
    assert ok == "pong@carol"

    for route, expected in ((["bob", "alice"], ["bob", "alice", "bob"]),
                            (["bob", "carol", "alice"], ["carol", "alice", "bob", "carol"])):
        start = time.monotonic()
        try:
            system.ask("alice", ("relay", route), timeout=5.0)
            raise AssertionError("deadlock not detected")
        except DeadlockDetectedException as exc:
            elapsed = (time.monotonic() - start) * 1000
            print(f"  → {' -> '.join(exc.cycle)} detected, returned in {elapsed:.0f} ms "
                  f"(without the detector it would hang until the 3000 ms timeout)")
            assert exc.cycle == expected and elapsed < 1000

    assert wait_until(lambda: system.wait_graph.snapshot() == {})
    pongs = [system.ask(p, ("relay", []), timeout=3.0) for p in ("alice", "bob", "carol")]
    print(f"  → wait-for graph is empty, all actors restarted and responding: {pongs}")
    system.shutdown()


def chaos_dead_letter() -> None:
    banner("CHAOS 6: Root supervisor gives up — pending asks become dead letters, no timeout")
    system = ActorSystem()
    system.spawn("root", lambda: Supervisor([ChildSpec("doomed", CounterActor)], max_restarts=1))
    assert wait_until(lambda: system.get("doomed") is not None)
    system.send("doomed", "crash")
    system.send("doomed", "crash")
    start = time.monotonic()
    try:
        system.ask("doomed", "incr", timeout=3.0)
        raise AssertionError("no dead letter produced")
    except DeadLetter as exc:
        elapsed = (time.monotonic() - start) * 1000
        print(f"  → ask failed with DeadLetter in {elapsed:.0f} ms: {exc}")
        assert elapsed < 1000
    system.send("doomed", "incr")
    assert wait_until(lambda: [e[0] for e in system.escalations] == ["root"])
    print(f"  → dead letters = {len(system.dead_letters)}, escalations: {[e[0] for e in system.escalations]}")
    assert len(system.dead_letters) == 2
    system.shutdown()
    try:
        system.ask("root", "ping", timeout=3.0)
        raise AssertionError("ask after shutdown was not a dead letter")
    except DeadLetter:
        print("  → ask after shutdown also failed fast with DeadLetter")


def main() -> None:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(relativeCreated)7.0fms %(threadName)-14s %(levelname)-8s %(message)s")
    chaos_db_crash()
    chaos_load_with_random_failures()
    chaos_all_for_one()
    chaos_escalation()
    chaos_deadlock()
    chaos_dead_letter()
    banner("ALL CHAOS TESTS PASSED")


if __name__ == "__main__":
    main()
