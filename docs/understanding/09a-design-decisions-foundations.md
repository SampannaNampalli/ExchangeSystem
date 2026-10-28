# 09a — Design decisions, part 1: the foundations (records 1–26)

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** decision records 1–26: the original design (D1–D62) and the survey fixes D63–D65, each with what later revised it | **Prerequisites:** 01, 05, 09 (index) | **Related:** 09b, 07-modules/*, 08, 11, 15
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

Unusually for a codebase you inherit, this one comes with its decisions written down. `docs/ExchangeServer2_Spec.md` §3 numbers them **D1–D77** with a reason each, and code comments cite them ("(D44)"). D1–D43 come from the original project's Spec v8 (in the user's earlier project, `Exchange Server/src/docs/ExchangeServer_Spec_v8.md`); D44–D62 were made in the rewrite; D63–D65 were added on 2026-10-27 while these guides were being written, to fix problems the survey found; **D66–D77** were added the same day when the user asked for every risk in guide 11 to be fixed. This part covers records 1–26 (the foundations and D63–D65); **part 2 (guide 09b)** covers D66–D77. Where a later decision changed a record here, the record says so in a **Revised by** line.

So this guide does not have to *guess* most reasons. It labels each part:
- **[Verified — Spec Dnn]**: the Spec states this reason.
- **[Verified — path:line]**: the code or a comment states it.
- **[Inferred]**: my reconstruction (alternatives and trade-offs are mostly this).
- **⚠ Gap**: where the stated reason looks incomplete or rests on an untested assumption.

Each record has the same shape: **Decision · Why · Alternatives not taken · Trade-offs and consequences**.

Vision §3 sets the bar the author held itself to: "'Production systems do it' is not a reason unless true; the real reason is often simpler" [Verified]. Read the records with that in mind.

### Index

| # | Decision | Spec |
|---|---|---|
| 1 | One writer thread owns all exchange state | D21, D55, Vision §5 |
| 2 | Journal the inputs, not the state | D12, D41 |
| 3 | Plain-text journal | D36 |
| 4 | Write-ahead, one fsync per batch, smart batching | D37 → D52 |
| 5 | Replay through the same engine; determinism rules | D12, D34, D40, D41 |
| 6 | The Sequencer is the only clock reader and counter | D32, D33, D55 |
| 7 | Packages enforce book ownership | D1, D3, D44 |
| 8 | TreeMap of levels + linked list per level + id map; no cached best pointer | D5–D9, D59 |
| 9 | Eager removal of empty levels | D10, D14, D18 |
| 10 | Stateless checks at the gateway, state-dependent in the engine, and the latter journalled | D13, D19, D47 |
| 11 | Client order id unique per participant, forever | D45, D49 |
| 12 | Price band and market collar fixed at arrival | D50 |
| 13 | Poison pills: quarantine live, stop at startup | D51 |
| 14 | Rules version in the journal header | D20, D46 |
| 15 | Two-pass streaming replay | D42, D60 |
| 16 | Reads go through the queue | D57 |
| 17 | Bounded session queues, slow consumers disconnected | D54 |
| 18 | Unbounded queue, capacity checked under the hand-in lock | D56 |
| 19 | Callers get their own future | D61 |
| 20 | Bounded SUBSCRIBE snapshot | D62 |
| 21 | Fail-stop; only the writer closes the journal | D39, D58 |
| 22 | Text line protocol, thread-per-connection | D53, Spec §6 |
| 23 | Close a session only after its requests are answered | D63 |
| 24 | Check the journal header before changing the file | D64 |
| 25 | One process per journal: a lock file | D65 |
| 26 | Build and dependencies: Maven wrapper, no runtime libraries, Spring Boot dropped | Vision §8 |

Records revised by the risk fixes (details in guide 09b): 2 and 15 (snapshots, D71), 3 (checksums, D69), 11 (client order ids per journal, D71), 12 (collar before the first trade, D66), 13 (rebuild from a snapshot; double SKIP refused, D71/D77), 14 (crossing a rules change, and CHECK digests, D70/D71), 17 (missed fills recoverable, D73), 18 (per-session limits, D75), 19 (listener before the future, D76), 22 (authentication, TLS, timeouts, D74/D75), 23 (replaced by an in-flight count, D76), 25 (the lock becomes `JournalLock`, D71).

---

## 1. One writer thread owns all exchange state

**Decision.** Exactly one thread, `exchange-writer`, owns `Exchange`, `Sequencer`, `CommandJournal`, `MatchingEngine` and `OrderBook`. Any thread hands requests in through one queue and gets a result back through a future or callback. Nothing inside the engine or book is synchronised [Verified — `src/main/java/exchangeserver/core/ExchangeService.java:24-28`; Vision §5].

**Why.** [Verified — Vision §4.4, §5; `src/main/java/exchangeserver/command/Sequencer.java:16-17`] Determinism: with one thread, sequence order equals queue order equals processing order, with no locking around the sequencer. The comment's phrasing: "there is no second thread there to synchronise with." This is the pattern LMAX made well known [Inferred: general knowledge, not stated in the repo].

**Alternatives not taken.** [Inferred]
- Lock the book (`synchronized` methods or a `ReentrantReadWriteLock`): every caller pays for contention, and the processing order would come from lock acquisition order, so the sequencer would need its own lock to keep it consistent with the journal.
- Multi-threaded matching (e.g. partition by instrument): irrelevant for a single instrument. Matching on several threads in one process stays a non-goal (Vision §6); partitioning by instrument is planned instead as separate single-writer shards (Vision §7 step 24).
- An `ExecutorService` with one thread: equivalent; the threads guide the author studied compares a bare thread with a single-thread executor (section 4.2). The bare thread gives explicit control of the stop and halt states [Inferred].

**Trade-offs and consequences.**
- The engine is simple and lock-free; correctness reduces to "only the writer touches it" [Verified].
- Throughput is capped by one core. Measured at ~1 µs per command, the core is not the bottleneck; fsync is (Spec §5) [Verified].
- Everything the writer runs (reply callbacks, the gateway's `onOutput`) must never block, or the whole exchange stalls. That drives decisions 17 and 19 [Verified — `src/main/java/exchangeserver/core/OutputListener.java:9-14`].
- Even reads must go through the queue (decision 16), and tests must inspect state through a future [Verified — CLAUDE.md].

---

## 2. Journal the inputs, not the state

**Decision.** The source of truth is the ordered list of **stamped input commands** (`NEW`, `CANCEL`, `SKIP`). Book state is never saved; on restart it is rebuilt by feeding the commands through the engine, and those replay outputs are discarded [Verified — Spec D12, D41; `src/main/java/exchangeserver/command/Command.java:5-9`].

**Why.** [Verified — Vision §4.5 "Inputs are the source of truth. Log sequenced commands; derive outputs by replay."] One code path rebuilds state (the engine itself), so there is no second "load state" path that could disagree with it.

**Alternatives not taken.** [Inferred]
- Journal outputs/events (trades, book changes) and rebuild by applying them: needs a second "apply event" path, but recovery doesn't depend on engine determinism.
- Periodic state snapshots plus a journal tail: the standard way to bound recovery time. Not built in the original design; **now built as offline compaction (D71, guide 09b)**, which keeps this decision intact: the snapshot is a checkpoint of derived state, and the journal of inputs is still the truth.
- A database: far beyond scope; the goal is to learn the mechanism.

**Trade-offs and consequences.**
- Replay is only correct if the engine is deterministic (decision 5) and the rules haven't changed (decision 14) [Verified]. Since D70, CHECK records detect a replay that produces different outputs.
- Recovery time and the client-order-id map grew with the whole history until D71: compaction now bounds both [Verified — Spec D71].
- An engine bug that crashes on a command crashes replay too, which is why the poison-pill policy exists (decision 13) [Verified — Spec D51].

---

## 3. Plain-text journal

**Decision.** One record per line, comma-separated, `\n`-terminated; timestamps as `Instant.toString()`; enums by name; a MARKET order's price as `-` [Verified — Spec D36, §4; `src/main/java/exchangeserver/journal/JournalCodec.java:20-36`]:

```
HEADER,3,2,0,5d7826e0
NEW,1,2026-10-27T14:20:40.373493200Z,1,alice,a1,SELL,LIMIT,10,100,379beca7
NEW,3,2026-10-27T14:20:41.217763100Z,3,bob,b1,BUY,LIMIT,4,100,bed0c339
CANCEL_ALL,4,2026-10-27T14:20:42.045241200Z,alice,2b6bb4a5
```

(Format v3, observed live on 2026-10-27: the header now carries the base sequence, and every record ends with a CRC-32C, D69.)

**Why.** The Spec records the decision without a separate reason in v9 (it is inherited from v8) [Verified — Spec D36]. ⚠ Gap: the likely reasons are readability, debuggability with any text tool, and no schema tooling, which fits a learning project [Inferred].

**Alternatives not taken.** [Inferred] A binary format with length prefixes and per-record checksums (compact, fast, detects corruption); JSON lines (self-describing but larger); an existing log library (Chronicle Queue, Kafka).

**Trade-offs and consequences.**
- You can read and even hand-edit the journal; this is how the poison-pill story stays explainable [Inferred].
- **Revised by D69**: there was no per-record checksum, so a bit flip that still parsed changed history silently; now every record carries a CRC-32C and a mismatch stops startup [Verified — Spec D69].
- **LF only**, still: a tool that converts the file to CRLF (an editor, git `autocrlf`) makes it unreadable. Since D69 the error says so ("the file was converted to CRLF line endings") instead of a puzzling header error, and `.gitattributes` keeps `*.journal` out of line-ending conversion [Verified — `src/main/java/exchangeserver/journal/JournalCodec.java:171-176`; `.gitattributes`].
- Separators must not appear in fields, so `Rules` forbids `,` and whitespace in participant names and client order ids [Verified — `src/main/java/exchangeserver/domain/Rules.java:27-29`].
- Parsing costs a little CPU, which doesn't matter next to fsync [Inferred].

---

## 4. Write-ahead, one fsync per batch, smart batching

**Decision.** For each batch, the writer stamps every request, appends them all with **one write and one fsync**, and only then processes them. The batch is "whatever is already waiting", up to `maxBatchSize` = 1,000, with no timer ("smart batching"). A `lingerNanos` mode (wait a little for more) exists only so the benchmark can compare [Verified — Spec D52; `src/main/java/exchangeserver/core/ExchangeService.java:30-33, 286-340`].

**Why.**
- *Write-ahead*: "visible ⇒ durable": no reply, fill or market data exists for a command that is not on disk [Verified — Spec §2, §4].
- *Per batch rather than per command*: measured. Per-command fsync is capped at about one order per fsync (~0.8 orders/ms); smart batching reached 43 orders/ms at 64 clients with p50 latency still about one fsync (1.4 ms) [Verified — Spec §5].
- *Smart rather than linger*: same latency as per-command at light load (a batch of one), and no timer. On Windows a "1 ms" timed wait rounds up to the ~15.6 ms timer resolution, so linger measured *worse* than both [Verified — Spec §5].

**Alternatives not taken.**
- fsync per command (the v8 design, D37): rejected by measurement [Verified].
- Group commit with a timeout (linger): measured and rejected; kept in code for comparison [Verified].
- No fsync, relying on the OS page cache: fast, but a power loss loses acknowledged orders [Inferred].
- Asynchronous replication instead of fsync: out of scope (distribution is a later roadmap topic) [Verified — Vision §7].

**Trade-offs and consequences.**
- **Durable ⇒ visible does not hold**: if the process dies after the fsync and before replies go out, journalled commands whose clients heard nothing are replayed on restart. Clients reconcile by resending with the same client order id (decision 11) [Verified — Spec §4].
- A halt fails every command in the batch with `HALTED` although they are journalled [Verified — Spec §8].
- ⚠ Gap: the linger result is specific to Windows' timer. The Spec itself notes that on Linux linger would be closer to 1 ms but would still add that 1 ms to every lightly loaded request, so the conclusion likely holds [Verified — Spec §5], but it was not measured there.
- The linger code path stays in production code, used only by tests and the benchmark (see guide 11) [Verified].

---

## 5. Replay through the same engine; determinism rules

**Decision.** Replay, the live path and the poison-pill rebuild all call the same `MatchingEngine.onNewOrder`/`onCancel`. The engine reads no clock, keeps no counter, uses no randomness and never iterates a hash map; a trade's timestamp is the incoming command's timestamp [Verified — Spec D12, D34, D40, D41; `src/main/java/exchangeserver/engine/MatchingEngine.java:27-31`].

**Why.** [Verified — Vision §4.3–4.4] "One code path where possible" and "Determinism: engine behaviour depends only on the ordered input commands." The trade-timestamp rule's reason is spelled out: "reading the clock here would make replay produce different trades" [Verified — `src/main/java/exchangeserver/engine/Trade.java:10-12`].

**Alternatives not taken.** [Inferred] A separate replay/apply path, or letting the engine read time and accepting that replayed trades differ in timestamps (breaks equality checks of rebuilt state).

**Trade-offs and consequences.**
- Determinism is enforced by convention plus a test (`MatchingEngineTest.Determinism`), not by the compiler. A future change that, say, iterates a `HashMap` would break replay silently [Inferred].
- Outputs depend on iteration order in exactly one place, the set of touched levels, which is a `LinkedHashSet` so level updates come out in the order levels were touched [Verified — `src/main/java/exchangeserver/book/OrderBook.java:45-46`].

---

## 6. The Sequencer is the only clock reader and counter

**Decision.** `Sequencer` stamps sequence numbers (every command), order ids (new orders only) and timestamps. If the clock steps backwards, the previous timestamp is reused. It runs only on the writer thread. Requests in the queue are unstamped; stamping happens on the writer [Verified — Spec D32, D33, D55; `src/main/java/exchangeserver/command/Sequencer.java:9-19, 86-93`].

**Why.** [Verified — Spec D55] "Sequence order = queue order = processing order, with no locking around the sequencer; producers never read the clock." Timestamps never decreasing is also a replay check (decision 15).

**Alternatives not taken.** [Inferred] Stamping on the producer thread (needs a lock or atomic counter, and stamp order could differ from queue order); `AtomicLong` ids (same ordering problem); `System.nanoTime` (not wall-clock).

**Trade-offs and consequences.**
- The clock is injected (`java.time.Clock`), so tests drive time with `MutableClock`, including stepping it backwards [Verified — `src/test/java/exchangeserver/testing/MutableClock.java`].
- A stamping failure (invalid cancel) must not use up a number. `stampCancel` builds the command first and increments only once it is valid [Verified — `src/main/java/exchangeserver/command/Sequencer.java:54-58`].
- Ids are never reused, but gaps are normal (a quarantined command keeps its id; `SKIP` records use sequence numbers) [Verified — Spec D16].

---

## 7. Packages enforce book ownership

**Decision.** `OrderBook` owns every resting order's lifecycle. Every `PriceLevel` mutator (`addOrder`, `headNode`, `fillHead`, `unlink`) is package-private, so the engine (another package) can read levels but cannot change them. The engine calls `OrderBook.fillBest`, which fills, removes an exhausted order and an emptied level, and returns an immutable `Fill` [Verified — Spec D1, D3, D44; `src/main/java/exchangeserver/book/PriceLevel.java:17-18`].

**Why.** [Verified — Spec D44; Vision §4.2] "The compiler, not convention, keeps the engine out of book internals." It closes a tension from the original project, where an engine-side `fillHeadOrder` had to keep the id map and the list in step by hand.

**Alternatives not taken.** [Inferred] One big class (simpler but mixes matching policy with data structure); a public book API with documentation-only rules.

**Trade-offs and consequences.**
- The invariants (id map iff resting, no empty or crossed levels, open-order counts) each have one place that maintains them [Verified — `src/main/java/exchangeserver/book/OrderBook.java:21-30`].
- Tests need package-private seams (`ordersAt`, `checkInvariants`, `MatchingEngine.book()`), which forces test classes into the same packages [Verified].
- CLAUDE.md instructs future contributors not to widen visibility for convenience [Verified].

---

## 8. TreeMap of levels + linked list per level + id map; no cached best pointer

**Decision.** Bids are `TreeMap<Long, PriceLevel>` in reverse order and asks in natural order. Each level is a hand-written doubly linked list with head/tail **sentinels** (dummy nodes, so linking and unlinking never need edge-case branches). An `orderId → OrderNode` `HashMap` gives O(1) cancel. There is no cached pointer to the best level [Verified — Spec D5–D9, D59; `src/main/java/exchangeserver/book/OrderBook.java:41-46`; `src/main/java/exchangeserver/book/PriceLevel.java:9-15`].

**Why.**
- Linked list + node handle: cancel and fill-removal of any order is O(1) without searching (Vision §4.8) [Verified].
- No cached best pointer: measured. `TreeMap.firstEntry` costs 8–18 ns at 10–100,000 levels, under 2% of a ~1 µs command (D59) [Verified — Spec §5].

**Alternatives not taken.**
- `java.util.LinkedList`/`ArrayDeque` per level: no stable node handles, so cancel would be O(n) [Inferred].
- Price-indexed arrays (common in real venues): need a bounded price range; "benchmarks did not ask for it" [Verified — Vision §6].
- A cached best-level pointer: rejected by measurement [Verified — D59].

**Trade-offs and consequences.** More code to get right (pointer surgery), so heavy testing: prev/next walks are cross-checked, and a random-traffic test re-derives every invariant after each of 30,000 commands [Verified — `src/main/java/exchangeserver/book/OrderBook.java:211-268`; `src/test/java/exchangeserver/book/OrderBookTest.java:293-337`].

---

## 9. Eager removal of empty levels

**Decision.** When a level's last order leaves (fill or cancel), the level is removed from the tree immediately, inside the single removal path, and the code checks that `TreeMap.remove` returned that very level [Verified — Spec D10, D14, D18; `src/main/java/exchangeserver/book/OrderBook.java:110-128`].

**Why.** [Verified — Vision §4.1] Invariant: "the book never contains empty price levels". The best level is then always a real price, so the match loop and FOK pre-check need no "skip empty levels" logic [Inferred].

**Alternatives not taken.** [Inferred] Lazy cleanup (leave empty levels and skip them on lookup); cheaper when a price is re-used quickly, but every reader must handle empties.

**Trade-offs and consequences.** An O(log L) tree removal each time a level empties; `add` must be all-or-nothing so a failed add never leaves an empty level behind [Verified — `src/main/java/exchangeserver/book/OrderBook.java:48-74`].

---

## 10. Validation layers; state-dependent rejects are journalled

**Decision.** Field validation happens **only** in `OrderRequest` (at the gateway, and again when the journal is decoded). `Order` can only be built from an `OrderRequest`. State-dependent checks (duplicate client order id, band, cap, post-only cross) run in the engine after journalling, so they are replayed. Stateless rejects never reach the queue [Verified — Spec D13, D19, D47; `src/main/java/exchangeserver/engine/Reason.java:3-10`].

**Why.** [Verified — Spec D47] In v8 the same rules existed in two places that had to be kept in step; now "an invalid `Order` is impossible by type, not by duplication". Journalling state-dependent rejects means replay reproduces them exactly (a reject still uses up an order id and, here, a client order id) [Verified — Spec D13, D49].

**Alternatives not taken.** [Inferred] Validate everything in the engine (wastes journal space and fsyncs on garbage); validate everything at the gateway (impossible for state-dependent rules without reading the book from another thread).

**Trade-offs and consequences.** The journal decoder re-runs today's stateless rules on old records, so tightening a stateless rule could make an old journal unreadable. That is one reason all such rules live in `Rules` behind `Rules.VERSION` [Verified — `src/main/java/exchangeserver/domain/Rules.java:5-16`].

---

## 11. Client order id unique per participant, forever (revised by D71: per journal)

**Revised by D71.** Since compaction exists, "forever" became "within one journal, plus the client order ids of orders still resting at the snapshot". The record below describes the original decision; its cost (a map that only grows) is what D71 bounds.

**Decision.** Every order carries a participant and a client order id (`clOrdId`). A repeat by the same participant is `REJECTED` with `DUPLICATE_CLIENT_ORDER_ID` **and the original order's id**. Every new order uses up its client order id, rejected ones included [Verified — Spec D45, D49; `src/main/java/exchangeserver/engine/MatchingEngine.java:183-187`].

**Why.** [Verified — Spec D49] It closes the "durable ⇒ visible" gap from the client side: a client that got no reply (crash, halt) resends after restart and learns the original's id instead of creating a duplicate order.

**Alternatives not taken.** [Inferred] Uniqueness per trading day (the Spec's own suggested fix); uniqueness only among *open* orders (the usual FIX behaviour, but then a resend after a fill would create a new order); server-side idempotency keys with expiry.

**Trade-offs and consequences.**
- The map only grows, so memory and replay cost grow forever [Verified — Spec §8]. It also feeds a deferred security finding (memory exhaustion) [Verified — user memory].
- A participant cannot correct a mistyped order by reusing its `clOrdId`, even after a reject [Verified — test `MatchingEngineTest.Risk.aRejectedOrderStillUsesUpItsClientOrderId`].
- ⚠ Gap: the check runs *first*, before the band and cap checks, and records the id even when a later check rejects. That ordering is required by "every new order uses up its id", but the Spec does not discuss whether rejected orders *should* consume ids [Inferred].

---

## 12. Price band and market collar fixed at arrival

**Decision.** A priced order must be within ±10% of the last trade price (inclusive), or it is `REJECTED` `PRICE_OUTSIDE_BAND`. A market order trades only inside the same range, computed once on arrival with floor/ceil rounding inward so it equals `withinBand` exactly. There is no band before the first trade [Verified — Spec D50; `src/main/java/exchangeserver/domain/Rules.java:42-47`; `src/main/java/exchangeserver/engine/MatchingEngine.java:270-290`].

**Why.** [Verified — Spec D50] Fat-finger protection and market-order price protection; "fixed at arrival so an order's own trades cannot widen its collar."

**Alternatives not taken.** [Inferred] Reference = mid or last close; dynamic collar moving with each trade; no protection.

**Trade-offs and consequences.**
- Exact integer math (`price * 100` vs `ref * 110`) avoids floating-point rounding; overflow is ruled out by `MAX_PRICE` [Verified — `src/main/java/exchangeserver/domain/Rules.java:42-47`].
- **Revised by D66**: before the first trade there used to be no protection at all, and a market order swept every level (the old test `withoutAReferencePriceSweepsEveryLevel` asserted exactly that). Now the collar is anchored on the opposite touch before the first trade; priced orders still have no band then [Verified — `src/main/java/exchangeserver/engine/MatchingEngine.java:270-290`; test `MatchingEngineTest.Market.beforeTheFirstTradeTheCollarIsAnchoredOnTheTouch`].
- A thin market can get "stuck": if the last trade is far from where liquidity now sits, priced orders near the new liquidity are rejected [Inferred].

---

## 13. Poison pills: quarantine live, stop at startup

**Decision.** Live, if the engine throws a `RuntimeException` on command *k*: append `SKIP k` (fsynced), rebuild the engine by replaying everything before *k* (minus earlier skips), continue the batch. The caller gets `INTERNAL_ERROR`; the listener never hears of *k*. An `Error` halts instead. At startup a failing record is **never** skipped automatically: startup fails and an operator runs `--quarantine <seq>` [Verified — Spec D51; `src/main/java/exchangeserver/core/Exchange.java:236-287`].

**Why.** [Verified — Spec D51] Live, *k*'s effects were never visible (outputs are emitted only after success), so history without *k* is exactly what clients saw. At startup the code cannot tell a poison pill from a record that succeeded under older code; skipping the latter would silently diverge.

**Alternatives not taken.** [Inferred] Halt on any engine exception (simplest, but one bad order stops the market); skip and continue without rebuilding (the engine may be half-mutated); delete the bad record (edits history; the design rule is "annotate, never edit") [Verified — `src/main/java/exchangeserver/command/SkipCommand.java:6-13`].

**Trade-offs and consequences.**
- The rebuild replays the journal up to *k*; since D71 it starts from the snapshot the journal names, so its cost is bounded by the time since the last compaction, and it rebuilds the whole ledger (engine, output digest, private streams) [Verified — `src/main/java/exchangeserver/core/Exchange.java:278-287`].
- The operator tool used to append a `SKIP` even for an already-skipped command; it now refuses ("already quarantined", L3, D77) [Verified — `src/main/java/exchangeserver/core/Exchange.java:160-161`].
- The rebuild uses the feed pass alone with the skip set `Exchange` already holds, reading the journal once (review fix 9) [Verified — Spec D60, §9].
- At startup an engine failure now carries its sequence number (`ReplayException`, kind `ENGINE_FAILURE`), and `Main` prints the quarantine advice only in that case (D77).

---

## 14. Rules version in the journal header

**Decision.** The first journal record is `HEADER,<format>,<rulesVersion>`. `open` refuses a journal written under another rules version [Verified — Spec D20, D46].

**Why.** [Verified — Spec D46] "Replaying under changed rules would silently build a different book." D20's lesson (limits are code constants) is made mechanical.

**Alternatives not taken.** [Inferred] Store the rules in the journal; version every command; trust discipline.

**Trade-offs and consequences.**
- It catches deliberate rule changes, **not** accidental behaviour changes in code. **Revised by D70**: CHECK records now catch a behaviour change that alters any output, at the next CHECK [Verified — Spec D70].
- The refusal message used to say "Start a fresh journal from a snapshot" when snapshots did not exist. **Revised by D71**: it now says to compact with the build that wrote the journal; an empty compacted journal adopts the new rules version, which is how a rules change is crossed without losing the book [Verified — `src/main/java/exchangeserver/journal/CommandJournal.java:252-259`].

---

## 15. Two-pass streaming replay

**Decision.** Replay reads the file twice: pass 1 verifies (sequence numbers exactly base+1, base+2, …, where the base is the snapshot the header names, 0 without one; timestamps never decrease) and collects `SKIP`s and the counters the sequencer resumes from; pass 2 feeds every non-skipped `NEW`/`CANCEL` [Verified — Spec D42, D60; `src/main/java/exchangeserver/core/Replay.java:15-49`].

**Why.** [Verified — Spec D60] Constant memory, and nothing is fed if the history is invalid anywhere. A `SKIP` comes *after* the command it skips, so the skip set must be complete before feeding.

**Alternatives not taken.** [Inferred] Load the whole journal into a `List` (the v8 approach: simple, O(n) memory); single pass with lookahead.

**Trade-offs and consequences.** Twice the I/O at startup [Inferred]. `lastOrderId` comes from `NEW` records only, skipped ones included, because that id *was* issued [Verified — `src/main/java/exchangeserver/core/Replay.java:103`].

---

## 16. Reads go through the queue

**Decision.** `snapshot(depth)` is a non-journalled request processed by the writer in queue order. `SUBSCRIBE` takes the snapshot and joins the subscribers in one writer-thread step [Verified — Spec D57; `src/main/java/exchangeserver/core/Request.java:35-42`].

**Why.** [Verified — Spec D57] No other thread ever touches the book (no data race), and a subscriber sees no gap and no duplicate between snapshot and updates.

**Alternatives not taken.** [Inferred] A concurrent read-only view; copy-on-write snapshots published via `volatile`.

**Trade-offs and consequences.** A read waits behind queued writes and costs writer time, hence decision 20's depth bound [Verified]. The risk fixes added two more reads that use the same "between two commands" property: `Attach` (a session joins its participant's private stream with no gap) and `Resend` (D73) [Verified — `src/main/java/exchangeserver/core/Request.java:44-55`].

---

## 17. Bounded session queues; slow consumers disconnected

**Decision.** Each session's outbound queue is an `ArrayBlockingQueue` of 10,000 lines. The writer only `offer`s; a full queue disconnects that client (D54) [Verified — `src/main/java/exchangeserver/gateway/ClientSession.java:115-130`].

**Why.** [Verified — Spec D54] "One slow client must not stall the exchange for everybody (a classic exchange failure mode)."

**Alternatives not taken.** [Inferred] Blocking `put` (stalls the exchange); an unbounded queue (memory grows without limit); dropping lines (corrupts the client's view).

**Trade-offs and consequences.** A disconnected client loses the lines still queued. **Revised by D73**: its private events (replies, fills, cancels) can now be recovered with `RESEND` after reconnecting, up to 1,000 per participant; market data has no resend (re-SUBSCRIBE gives a fresh snapshot) [Verified — Spec D73, §8]. The slow-consumer disconnect itself still has no automated test (guide 10) [Verified].

---

## 18. Unbounded queue, capacity checked under the hand-in lock

**Decision.** The request queue is an unbounded `LinkedBlockingQueue`; the capacity (100,000) is checked inside `synchronized (lock)` together with the state check, and a full queue refuses with `BUSY`. `stop()` flips the state and enqueues `STOP` under the same lock [Verified — Spec D56; `src/main/java/exchangeserver/core/ExchangeService.java:210-258`].

**Why.** [Verified — Spec D56] "Refused" and "never lost" at once: nothing can be accepted after `STOP`, and `stop()` never blocks on a full queue. Refusing rather than blocking keeps gateway threads responsive and makes backpressure visible.

**Alternatives not taken.** [Inferred] A bounded queue with blocking `put` (a producer blocks forever if the writer has died, and `stop()` could block on a full queue); bounded with `offer` (then `STOP` might not fit).

**Trade-offs and consequences.** One global capacity, so one aggressive client could make everyone `BUSY` (deferred finding 4). **Revised by D75**: each session may now have at most 1,000 requests in flight and is rate-limited, so one client can no longer fill the queue alone [Verified — Spec D75]. Tested with 5 × 1,000 racing requests (`racingStopNeverLosesARequest`) [Verified].

---

## 19. Callers get their own future (D61)

**Decision.** `submit`/`cancel`/`snapshot` return a *separate* `CompletableFuture`, completed from the common pool via `whenCompleteAsync`. The writer decides what to process from its own state (`Pending.refused`), never from a future's. The callback variants (used by the gateway) still run on the writer and must not block [Verified — Spec D61; `src/main/java/exchangeserver/core/ExchangeService.java:143-208`; `src/main/java/exchangeserver/core/Exchange.java:225-230`].

**Why.** [Verified — Spec D61, §9 findings 1 and 8] Found in code review: with a shared future, a caller's `cancel()` or `orTimeout()` on a queued request made the writer skip a command it had already journalled (live book ≠ replayed book), and any continuation a caller attached ran on, and could block, the writer.

**Alternatives not taken.** [Inferred] Return `future.copy()` (Java 9+; similar effect); document "don't cancel" (unenforceable).

**Trade-offs and consequences.** One extra future and a pool hop per request on the future API; the gateway uses callbacks. **Revised by D76**: the callback used to run *before* the listener (that was how the reply preceded market data); now the reply travels in the participant's private stream through the listener (D73), and the callback runs *after* it, so a session can count requests in flight and close when they are done [Verified — `src/main/java/exchangeserver/core/Exchange.java:246-251`].

---

## 20. Bounded SUBSCRIBE snapshot (D62)

**Decision.** `SUBSCRIBE` snapshots at most 100 levels per side [Verified — Spec D62; `src/main/java/exchangeserver/gateway/Protocol.java:48-54`].

**Why.** [Verified — Spec D62] Bounds the writer-thread cost and the line length whatever the book depth (a client could loop SUBSCRIBE/UNSUBSCRIBE). `LEVEL` updates carry absolute quantities, so a deeper level appears in a subscriber's view the first time it changes.

**Trade-offs.** A subscriber's view of deep levels is incomplete until they change [Verified].

---

## 21. Fail-stop; only the writer closes the journal

**Decision.** A journal I/O failure marks the journal failed (every later append refused); any `Throwable` escaping the writer loop halts the service; only the writer closes the journal, as its last act; a halted process exits 2 [Verified — Spec D39, D58; `src/main/java/exchangeserver/core/ExchangeService.java:286-325`].

**Why.** [Verified — Vision §4.10; Spec D58] "Fail-stop over limping"; the journal is the truth on restart. "Nothing can be using the journal when it closes."

**Alternatives not taken.** [Inferred] Retry fsync (unsafe: after a failed fsync, Linux may have dropped the dirty pages, so a retry can report success for data that was lost); keep serving reads after a journal failure.

**Trade-offs.** Requires an external supervisor to restart the process; none is provided [Inferred].

---

## 22. Text line protocol, thread-per-connection

**Decision.** ASCII lines, `TYPE key=value ...` outbound; FILL lines before the ORDER outcome; market data after the reply for the same command. One reader and one sender thread per connection, plus an acceptor; at most 256 sessions [Verified — Spec D53, §6].

**Why.** [Verified — Spec D53] "Self-describing, extensible, debuggable with any line client." The ordering is "received → matches → done". ⚠ Gap: thread-per-connection has no stated reason beyond being the simple choice; NIO or virtual threads are planned as roadmap step 13 [Verified — Vision §7].

**Alternatives not taken.** FIX (non-goal, Vision §6); binary protocols; NIO selectors; Java 21 virtual threads (the project targets 17) [Verified/Inferred].

**Trade-offs.** 512 platform threads at the session cap. Idle connections used to hold threads forever; **revised by D74/D75**: login and idle timeouts, authentication with a secret, optional TLS, per-session limits, and a loopback bind by default [Verified — Spec D74, D75].

---

## 23. Close a session only after its requests are answered (D63, revised by D76)

**Revised by D76.** The goal stands; the mechanism below (a snapshot read used as a barrier) was replaced the same day by a per-session in-flight count, because the barrier failed during shutdown (see guide 09b, record 37). The record keeps the original design for history.

**Decision (original).** When the reader stops (QUIT, end of stream, overlong line) it queued a depth-1 snapshot read; that read's callback sent `BYE` (for QUIT) and closed the session. A broken connection (I/O error) still closes at once [Verified — Spec D63; the current reader is `src/main/java/exchangeserver/gateway/ClientSession.java:152-182`].

**Why.** [Verified — Spec D63] The queue is FIFO, so that read completes after every request the session handed in, and their reply callbacks have already offered their lines. Before this, QUIT sent `BYE` and closed at once: a client sending `NEW` then `QUIT` lost the `ORDER` reply (199 of 200 in the regression test, run against the old code) for an order that executed and may rest.

**Alternatives not taken.** [Verified — Spec D63] A dedicated "barrier" request type in `core` (more API surface); per-session in-flight counters [Inferred].

**Trade-offs.** One tiny snapshot per disconnect on the writer. During shutdown the read was refused immediately and the session closed without waiting, losing the replies of requests accepted before the stop. That flaw is why D76 replaced the barrier [Verified — Spec D76; test `TcpGatewayTest.Session.quitDuringShutdownStillGetsTheRepliesOfAcceptedRequests`].

---

## 24. Check the journal header before changing the file (D64)

**Decision.** `CommandJournal.open` verifies the file first. A complete first record must be a header of this rules version; a file with no complete record must be a prefix of the expected header (a crash while creating the journal). Otherwise it refuses and the file is untouched. Only then does it cut a torn tail or write a new header [Verified — Spec D64; `src/main/java/exchangeserver/journal/CommandJournal.java:75-127, 241-260`]. Since D71 one exception exists: a journal **with no records** written under another rules version is accepted and its header rewritten (atomically) with this build's version, which is how a rules change is crossed after compaction.

**Why.** [Verified — Spec D64] Before, the torn-tail cut ran first: pointing `--journal` at any other file cut its unterminated last line, and a one-line file without a newline was emptied and turned into a journal. A journal written under other rules was also cut before being refused. All three cases have regression tests that fail on the old code.

**Trade-offs.** A slightly stricter open: a torn header from a *different* rules version is refused rather than recreated [Inferred from `checkHeader`].

---

## 25. One process per journal: a lock file (D65)

**Decision (current).** Acquiring the lock first adds the lock file's real path to an in-JVM set (`HELD`), then takes an exclusive `FileLock` on a separate file `<journal>.lock` through a channel nothing else ever opens. A second open, in the same JVM or another process, is refused with "already open". Closing the journal closes its channel, then releases the lock. The empty lock file stays on disk and is gitignored (`*.journal.lock`). Since D71 the lock is its own public class, `JournalLock`, so compaction can hold it while it replaces the journal file [Verified — `src/main/java/exchangeserver/journal/JournalLock.java:13-67`; `.gitignore`; Spec D65, D71].

```java
// src/main/java/exchangeserver/journal/JournalLock.java:37-55 (trimmed)
public static JournalLock acquire(Path journal) throws IOException {
    Path lockFile = journal.resolveSibling(journal.getFileName() + ".lock");
    try {
        Files.createFile(lockFile);     // create-new: never opens, so never closes, an existing lock file
    } catch (FileAlreadyExistsException normalAfterTheFirstRun) {
        // reuse it
    }
    Path key = lockFile.toRealPath();   // one key however the journal path is spelled
    if (!HELD.add(key)) {
        throw alreadyOpen(journal);
    }
    FileChannel channel = null;
    try {
        channel = FileChannel.open(lockFile, WRITE);
        if (channel.tryLock() == null) {
            throw alreadyOpen(journal);
        }
        return new JournalLock(key, channel);
```

**Why.** [Verified — Spec D65] Nothing stopped `--quarantine` from appending next to a running exchange. That interleaves sequence numbers, and the next startup fails on the gap or repeat.

**History: the first implementation was replaced the same day.** The first D65 locked one byte at `Long.MAX_VALUE - 1` *of the journal file itself*. The byte was far past the data because Windows locks are mandatory, and locking the records would have blocked `read()` and the torn-tail cut, which use their own handles. It passed tests and a live check on Windows. A later review while writing these guides found it would not hold on Linux/macOS. There Java's file locks are POSIX locks, and **closing any handle a process holds on a file drops all of that process's locks on it**; `open`, `read()` and the torn-tail cut open and close handles on the journal, so the lock would vanish almost at once [Verified — Spec D65; `src/main/java/exchangeserver/journal/JournalLock.java:18-22`; the `FileLock` Javadoc warns of this platform dependence]. The fix moved the lock to a separate file that nothing else opens. For the same reason, a second in-JVM open must be refused by `HELD` *before* it opens the lock file at all. Otherwise closing the refused channel would drop the owner's POSIX lock.

There was also a **process lesson** from the first version. The first live check of it appeared to fail (`--quarantine` succeeded against a running server). The cause was a stale jar: restored sources kept older timestamps, and Maven's incremental compiler skipped them. A clean build showed the lock working on Windows. CLAUDE.md now warns about this [Verified — CLAUDE.md "Environment"].

**Alternatives not taken.** [Inferred] A lock on the journal file itself (the first version: platform-dependent); a PID file (stale after a crash; needs liveness checks); OS advisory locks via `flock` through JNI (no portable Java API).

**Trade-offs and consequences.**
- An extra file next to every journal; it is never deleted, because deleting it would race with the next opener [Verified — `src/main/java/exchangeserver/journal/JournalLock.java:22`].
- Tested across processes: a child JVM runs `Main --quarantine` while the test holds the journal and has already called `read()`, and must exit 1 with "already open" [Verified — `src/test/java/exchangeserver/journal/CommandJournalTest.java:312`]. It passed on Windows and, since the risk fixes, on **Linux** too (the whole suite, 446 tests, in an `eclipse-temurin:17` Docker container), and a live `--quarantine` against a running server was refused there [Verified — run on 2026-10-27]. ⚠ Gap: macOS has still not been run.
- Locks on network filesystems are unreliable in general [Inferred: general knowledge].

---

## 26. Build and dependencies

**Decision.** A Maven project with the wrapper (`mvnw`/`mvnw.cmd`, Maven 3.9.16 downloaded on first use), Java 17, `-Xlint:all` with a warning-free build. **No runtime dependencies**: JUnit 5, JMH and PIT are test- or profile-scoped. Spring Boot was dropped on 2026-10-24 in favour of a plain TCP gateway [Verified — `pom.xml:13-52`; `.mvn/wrapper/maven-wrapper.properties`; Vision §8].

**Why.** [Verified — Vision §8 records the decision.] ⚠ Gap: no reason is written down for dropping Spring Boot. Likely reasons: a framework hides exactly the threading, I/O and lifecycle mechanics the project exists to teach, and adds startup time and dependencies [Inferred]. The wrapper means "no local Maven needed" [Verified — README].

**Trade-offs.** Hand-written networking, logging setup and CLI parsing (`Main` parses its flags by hand: fifteen of them since the risk fixes) [Verified — `src/main/java/exchangeserver/Main.java:66-91`]. TLS and password hashing come from the JDK itself (JSSE, `PBKDF2WithHmacSHA256`), so the fixes added no dependency [Verified — `src/main/java/exchangeserver/gateway/Tls.java`, `Credentials.java`]. A tiny jar (~110 KB) that runs with just a JDK [Verified — observed build].

---

## Key takeaways

- Most "why" questions have a written answer: look up the D-number in Spec §3 before guessing.
- The core triad: **single writer** + **write-ahead input journal** + **deterministic engine replayed through the same code**. Most other decisions protect one of these three.
- **Measured, not assumed**: smart batching over fsync-per-command and linger (D52); no cached best pointer (D59).
- **Visible ⇒ durable holds; durable ⇒ visible does not**. Unique client order ids (D49, now per journal after D71) and RESEND (D73) are the client-side remedies.
- **Compiler-enforced ownership** (D44) and **one place per invariant** keep the book correct.
- **Poison pills**: quarantine live (safe, because nothing was visible), refuse at startup (can't tell a poison from history).
- **D61–D65 were found by review or survey**, not by design up front. Two of them (D63, D64) fixed silent data or reply loss.
- **D65 changed implementation the same day**: a lock on the journal itself would not hold on POSIX systems; the lock moved to `<journal>.lock` and is now tested on Windows and Linux.
- **Many records here were revised by D66–D77** (guide 09b): no checksum → CRC; no snapshots → compaction; forever-unique ids → per journal; no behaviour-change detection → CHECK digests; D63's barrier → an in-flight count.
- Gaps still to remember: LF-only journal, no stated reason for dropping Spring Boot or for thread-per-connection.

## Common confusions

- *"The journal stores the book."* It stores **inputs**; the book is always derived by replay.
- *"Smart batching waits to fill batches."* It never waits: it takes what is already queued. Linger (group commit with a timeout) is the mode that waits, and it lost the measurement.
- *"Rejected orders aren't journalled."* State-dependent rejects are (they consume ids and must replay); only stateless rejects never reach the journal.
- *"D65 is a lock on the journal file."* Not any more: the first version was; the current one locks the separate `<journal>.lock` file, for POSIX reasons.
- *"`Rules.VERSION` protects against any code change."* Only against deliberate changes to rule constants; since D70, CHECK digests catch a behaviour change that alters outputs.
- *"A snapshot replaces the journal as the source of truth."* It is a checkpoint of derived state; the archived journal and the new one still hold every input (D71).
- *"The poison-pill policy skips bad orders at startup."* It deliberately does **not**; an operator decides.

## Questions to test yourself

1. Why does the engine take a trade's timestamp from the incoming command instead of the clock?
<details><summary>Answer</summary>Replay feeds the same commands later; reading the clock would give different timestamps, so replayed trades would differ from live ones (Spec D34; `Trade.java:10-12`). Determinism is what makes "rebuild by replay" correct.</details>

2. What two problems did D61 fix, and what is the one rule that fixes the first?
<details><summary>Answer</summary>(1) A caller cancelling or timing out its future made the writer skip a journalled command, so the live book diverged from the replayed one; (2) caller continuations ran on, and could block, the writer. Rule: the writer decides from its own state (`Pending.refused`), never from a future's state; callers get a separate future completed on the common pool.</details>

3. The capacity check is under a lock but the queue itself is unbounded. Why not just use a bounded queue?
<details><summary>Answer</summary>So `stop()` can always enqueue `STOP` without blocking on a full queue, and so the state check and the enqueue are atomic with `stop()`'s state flip: every accepted request is ahead of `STOP` and nothing is accepted after it (D56).</details>

4. Why is a poison pill quarantined automatically live but never at startup?
<details><summary>Answer</summary>Live, the failing command's effects were never visible, so history without it equals what clients saw. At startup, a failing record might have succeeded under older code, and skipping it would silently diverge; an operator must decide (D51).</details>

5. Why did the first D65 lock byte sit at `Long.MAX_VALUE - 1`, and why was that design replaced?
<details><summary>Answer</summary>Windows locks are mandatory, so locking real journal bytes would block the journal's own `read()` and torn-tail cut; a byte far past the end avoids that. It was replaced because on Linux/macOS locks are POSIX locks that vanish when the process closes *any* handle to the file, and the journal code opens and closes such handles all the time. The lock moved to `<journal>.lock`, which nothing else opens.</details>

6. Name two decisions that were settled by measurement and the numbers behind them.
<details><summary>Answer</summary>D52 smart batching: 43 orders/ms at 64 clients vs ~0.8 for fsync-per-command, p50 ~1.4 ms. D59 no cached best pointer: `firstEntry` 8–18 ns, under 2% of a ~1 µs command.</details>

7. What did D49 cost, and how was it fixed?
<details><summary>Answer</summary>The client-order-id map grew forever (memory and replay time) and fed a memory-exhaustion risk. D71 scoped ids to one journal: `--compact` writes a snapshot that keeps only the resting orders' ids and starts an empty journal, typically at the end of a trading day.</details>
