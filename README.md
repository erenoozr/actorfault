# ActorFault

A fault-tolerant actor system with a live deadlock detector, written in pure Python 3.10+ (standard library only).

## Features

- **Actors with isolated mailboxes** — each actor runs on its own thread and processes messages from its own `queue.Queue`. Lifecycle states: `STARTING`, `RUNNING`, `CRASHED`, `STOPPED`.
- **Messaging** — fire-and-forget `send(target_id, message)` and synchronous `ask(target_id, message, timeout)` backed by a future.
- **Supervision trees** — supervisors are actors themselves and can be nested.
  - `OneForOne`: restart only the crashed child.
  - `AllForOne`: stop and restart the whole sibling group together.
  - Restart intensity: more than 3 crashes within 5 seconds makes the supervisor give up and escalate to its parent.
- **Mailboxes survive restarts** — a restarted actor is a fresh instance that inherits the queued messages, so nothing waiting in the queue is lost.
- **Live deadlock detection** — every `ask` adds an edge to a wait-for graph. Before the edge is added, a DFS checks whether it would close a cycle (`A -> B -> A`). If so, `DeadlockDetectedException` is raised immediately instead of freezing until timeout, and it propagates back through every actor in the cycle.
- **Dead letters** — messages to actors that are permanently dead (supervisor gave up, or system shut down) are recorded in `system.dead_letters`, and pending `ask` calls fail fast with `DeadLetter`.

## Example actors

| Actor | Role |
|---|---|
| `DatabaseActor` | Durable key/value storage; simulates random transient `ConnectionError`s |
| `CacheActor` | Write-through cache in front of the database; retries on `ConnectionError` |
| `WorkerActor` | Receives client requests and forwards them to the cache |

## Running

```bash
python actorfault.py
```

No dependencies. The script runs a chaos test suite with built-in assertions:

1. **Database crash** — the database is crashed on purpose; its supervisor restarts it and the queued messages are still processed.
2. **Load with random failures** — 3 clients send 30 increments through Worker → Cache → DB while the database randomly fails; clients see no errors and the counter ends at exactly 30.
3. **AllForOne** — one sibling crashes and the whole group is reset.
4. **Escalation** — a child crashes 4 times in a row; its supervisor gives up and the parent rebuilds the subtree.
5. **Deadlock** — 2-actor and 3-actor cycles are detected in ~0 ms instead of hanging until the 3 s timeout; a non-cyclic chain is not flagged.
6. **Dead letters** — after the root supervisor gives up, pending and new requests fail immediately.

The run ends with `ALL CHAOS TESTS PASSED`.

## Design notes

- Cycle check and edge insertion happen under one lock, so concurrent `A -> B` and `B -> A` asks cannot both slip through.
- `stop()` cancels an actor's in-flight `ask`, so an `AllForOne` restart can't hang on a sibling that is waiting for the crashed actor.
- The message that crashed an actor is dropped (as in Erlang/Akka); its `ask` caller receives the exception.
- The cache only retries on `ConnectionError`, which the database raises before mutating data. It never retries on timeout, because the increment may already have been applied.
