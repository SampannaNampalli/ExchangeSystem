# ExchangeServer2 — Vision (v4)

**Supersedes:** `ExchangeServer_Vision_v3.md` (in the original `Exchange Server` project). Self-contained.
**Companion:** `ExchangeServer2_Spec.md` (exact code state, decisions, open threads).
**Rule:** the roadmap lives only here. The spec points to it and does not duplicate it.

---

## 1. Purpose

ExchangeServer is a Java trade-exchange matching engine built as a **hands-on way to understand production-grade
software systems**. Depth of understanding matters more than feature count: every design choice should be
justified, tested, and understood well enough to defend.

**Honest scope note:** building something that *works like* a real exchange is achievable. Matching the scale
and robustness of NASDAQ/LMAX-class venues is not, solo. The aim is to hit the same design problems real systems
hit and understand why they were solved the way they were.

Learning order: correctness of the core -> concurrency model -> networking -> durability/recovery ->
(conceptually) distribution -> operability.

---

## 2. What the system is

A single-instrument limit order book with a price-time-priority matching engine, behind the full pipeline:

```
 clients --> TCP gateway --> one ordered queue --> writer thread --------------------------------> OrderBook
             (stateless       (backpressure:       stamp (seq, ts, id) -> journal (one fsync      (levels, nodes,
              validation)      BUSY when full)      per batch) -> engine (deterministic)            id map)
                                                            |                        |
                                                            v                        v
                                                    replay on restart        replies, passive fills,
                                                    (+ poison-pill skip)     market data (L2 + trades)
```

**Status (2026-10-27): roadmap steps 1–7 are implemented** in ExchangeServer2, a rewrite of ExchangeServer into a
Maven project with real packages. Steps 1–3 were rebuilt with the open threads of Spec v8 closed; steps 4–7 are
new. The rewrite was written by the LLM at the user's request (see §3).

---

## 3. Working style (how to collaborate)

1. **Reason before code.** Walk a concrete numeric scenario in plain English before generalizing.
2. **Who writes what.** In ExchangeServer the user wrote the production code and the LLM reviewed, quizzed, and
   wrote the tests. **ExchangeServer2 was written entirely by the LLM on 2026-10-27, at the user's explicit
   request** ("I write it all", full roadmap). Going forward, the user decides per piece of work which mode applies;
   the default for new features remains the original loop below.
3. **Verify by tracing, not asserting.** Before calling anything "done", trace it with concrete inputs, including
   edge cases and exit paths.
4. **Call out weak justifications.** "Production systems do it" is not a reason unless true; the real reason is
   often simpler. Stale rationales must be fixed when the design changes.
5. **Check exhaustiveness explicitly.** Name the dimensions of a problem (e.g. 2 sides x 3 price relations = 6
   cases) rather than listing cases until it feels enough.
6. **The loop for user-written work:** the LLM gives a short description (no hints, no code) -> the user designs
   and codes -> the LLM reviews the saved code and shows where it breaks (location + concrete input, not the fix)
   -> the LLM tests the user's understanding -> the user fixes; repeat -> the LLM writes and runs the tests and
   reports mutation results.

*Instructions for an assisting LLM:* adopt the above.

---

## 4. Engineering principles

1. **Invariants first.** State the invariant, find the single place that maintains it, make violations fail loudly.
   - `totalVolume` equals the live remaining volume of the level's nodes at all times.
   - The id map contains an id iff that order rests in a level.
   - The book never contains empty price levels, and is never crossed after a command.
   - Invariants are protected at their source, not re-checked redundantly by callers.
2. **One owner per responsibility, enforced by the compiler.** `OrderBook` owns the order lifecycle; every
   `PriceLevel` mutator is package-private, so the engine (another package) can read levels but cannot change
   them. The engine decides what a match means; the book carries it out (`fillBest` returns an immutable `Fill`).
   *(Resolves the v3 `fillHeadOrder` tension.)*
3. **One code path where possible.** Fill-removal and cancel share `removeNode`/`unlink`; replay, the live path
   and the poison-pill rebuild all use the same engine.
4. **Determinism.** Engine behaviour depends only on the ordered input commands. Ids, timestamps and sequence
   numbers are stamped by the sequencer on the writer thread before journalling; no wall-clock, randomness or
   hash-order dependence in matching; one writer thread, enforced by construction.
5. **Inputs are the source of truth.** Log sequenced commands; derive outputs by replay.
6. **Validate at the right layer.** Stateless checks at the gateway (`OrderRequest`); state-dependent checks in the
   engine. Internal invariant violations throw; external bad requests return rejects.
7. **Measure before optimizing.** Done in step 6: the cached best-level pointer was measured and rejected; smart
   batching was measured and kept (Spec §5).
8. **Honest complexity claims.** Cancel / fill removal of a node O(1); removing an emptied level O(log L);
   best-level lookup O(log L) (measured: ~10–20 ns, under 2% of a command).
9. **Business logic and data structures stay separated.** Containers expose state; the engine decides its meaning.
10. **Fail-stop over limping.** A journal failure or an unexpected error halts the writer; the process exits;
    the journal is the truth on restart.

---

## 5. Key design commitments (detail in the Spec)

- Doubly linked list per price level with sentinel nodes and caller-held node handles; `orderId -> OrderNode` map;
  eager removal of emptied levels.
- Write-ahead input log, plain text, with a header naming the rules version; state-dependent rejects are logged,
  stateless rejects never reach the queue.
- **Concurrency:** one writer thread owns sequencer, journal, engine and book. Callers hand in requests through one
  queue and get a `CompletableFuture` each. No internal thread-safety in the book.
- **Batching:** smart batching (take whatever is waiting, one fsync, then process), chosen by measurement.
- **Poison pills:** quarantine + rebuild live; stop + operator quarantine at startup.
- **Gateway:** plain TCP, line protocol, thread-per-connection, slow consumers disconnected.

---

## 6. Non-goals (for now)

- Multi-instrument support, multi-threaded matching, distributed replication/consensus.
- Price-indexed array books (needs a bounded price range; benchmarks did not ask for it).
- Full FIX/ITCH compliance; authentication; TLS.
- Production hardening beyond the learning goals.

---

## 7. Roadmap — single source of truth

| # | Step | State |
|---|---|---|
| 1 | Close open threads and O(1) cancel | done (v3), rebuilt in ES2 |
| 2 | Order types / cancel flows | done (v3), rebuilt in ES2 |
| 3 | Sequencer + event log | done (v3), rebuilt in ES2 with batch append, header, streaming replay |
| 4 | TCP gateway with a single-writer engine thread (4A thread model, 4B gateway, 4C batching) | **done (ES2)** |
| 5 | Risk checks + market data | **done (ES2)**: client order ids + duplicates, price band, market collar, open-order cap; L2 updates, trades, snapshots |
| 6 | Benchmarking | **done (ES2)**: JMH; cached pointer rejected; smart batching kept |
| 7 | Crash recovery via replay, including poison-pill policy | **done (ES2)** |
| — | Cleanup: one test file per production class with `@Nested` groups | **done (ES2)** |

**Next candidates (not scheduled; pick with the user):**
1. Snapshots + journal rotation (bounded recovery time; scopes the client-order-id map to a trading day).
2. Output sequence numbers and resend requests (a reconnecting client recovers missed fills).
3. Self-trade prevention (needs a policy choice: cancel newest / oldest / both).
4. NIO or virtual-thread gateway (Java 21) instead of two platform threads per connection.
5. Distributed systems concepts (replication of the journal, failover), observability (metrics, structured logs).

---

## 8. Decisions confirmed since v3

- **Service layer:** Spring Boot dropped (2026-10-24); plain TCP gateway.
- **Build:** Maven (wrapper included), packages under `exchangeserver.*`, tests in `src/test/java`.

---

## 9. Glossary

- **Resting order:** sitting on the book (passive side, "maker").
- **Incoming / aggressive order:** being matched now ("taker"); may never rest.
- **Touch:** best bid and best ask levels.
- **Unlink:** splicing a node out of its level's list in O(1) given the node.
- **Sentinel:** dummy head/tail node that removes edge-case branching.
- **Write-ahead:** persisting a command before processing it.
- **Poison pill:** a logged command that crashes the engine and therefore every replay.
- **Quarantine:** a `SKIP` record that makes replay pass over a poison pill without editing history.
- **Smart batching:** the writer takes whatever is already queued, journals it with one fsync, then processes it.
- **Band / collar:** the price range around the last trade outside which priced orders are rejected (band) and
  market orders stop trading (collar).
- **Backpressure:** refusing (BUSY) instead of queueing without limit.
