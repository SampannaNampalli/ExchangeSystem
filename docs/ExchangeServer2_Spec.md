# ExchangeServer2 — Spec (v9)

**Supersedes:** `ExchangeServer_Spec_v8.md` (original project). Self-contained: nothing needs to be read from earlier specs.
**Companion:** `ExchangeServer2_Vision.md` (purpose, working style, principles, **roadmap**).
**Verified on 2026-10-27 against the working tree:** `mvnw test` compiles with `-Xlint:all` and no warnings,
**334 tests, all green** (after the survey fixes, §10; the earlier "326 green, three runs" hid a flaky test, §10 #1).
**Status:** roadmap steps 1–7 complete (Vision §7).

---

## 1. Session rules

1. Follow Vision §3. ExchangeServer2's code was written by the LLM at the user's request; for new work, ask which
   mode applies (LLM-written, or the user-writes / LLM-reviews loop).
2. **Tests:** one test class per production class, `@Nested` groups per behaviour; every wait in a concurrent test
   has a timeout; no `Thread.sleep` for synchronisation (one test uses a sleep *inside* a 300 ms linger window to
   shape a batch, never for correctness).
3. **Mutation testing** is automated: `mvnw -Pmutation test` (PIT). Report survivors and explain them (§7).
4. **How to run:** `mvnw.cmd test` (Windows) / `./mvnw test`. No local Maven needed; JDK 17.
5. **Environment facts:** Windows 11, Temurin JDK 17.0.20; default charset windows-1252 (the build sets UTF-8;
   the code always passes a charset); one fsync ~1.5 ms; timed waits round up to the ~15.6 ms Windows timer.
   **Java opens files on Windows with delete sharing, so deleting a file does not prove its handle was closed**
   (an early version of the tests assumed it did; PIT exposed it). Tests use `CommandJournal.openJournals()`.
6. **References in code comments** that point outside this repo: "threads guide 4.3 / 4.4 / Lab 6" =
   `Exchange Server/New Learnings/03_Threads_Visibility_and_Handoff.md` (sections 4.3 shutdown state machine,
   4.4 timeouts in tests, Lab 6 one future per request); "Spec v8", "v3" = `Exchange Server/src/docs/
   ExchangeServer_Spec_v8.md`, `ExchangeServer_Vision_v3.md` (both in the original project). "Step N" / "step 4A"
   = the roadmap in Vision §7.

---

## 2. Current state

Packages under `src/main/java/exchangeserver/`. Single instrument.

| Class | What it is |
|---|---|
| `domain.Side`, `domain.OrderType` | `BUY`/`SELL` (`opposite()`); `LIMIT`, `IOC`, `FOK`, `MARKET`, `POST_ONLY` (`canRest()`: LIMIT, POST_ONLY). |
| `domain.Rules` | Every rule replay depends on: `VERSION`, `MAX_ORDER_QUANTITY` (1e9), `MAX_PRICE` (`Long.MAX_VALUE / MAX_ORDER_QUANTITY`), participant pattern `[A-Za-z0-9_]{1,16}`, client-order-id pattern `[A-Za-z0-9_.-]{1,32}`, `PRICE_BAND_PERCENT` (10), `MAX_OPEN_ORDERS_PER_PARTICIPANT` (10,000), `withinBand(price, ref)` (exact integer math). |
| `domain.OrderRequest` | Stateless validation (the only field-validation layer, D47). Private constructor; factories `limit`, `ioc`, `fok`, `postOnly`, `market`, and `of(..., OptionalLong price)`. A market order has no price in the API (`getPrice()` is `OptionalLong`). `equals`/`hashCode`. |
| `domain.Order` | Constructed only from `(orderId, OrderRequest, timestamp)`; checks id > 0 and non-null timestamp. `getPrice()` throws for MARKET; `hasPrice()`; `fill(q)` rejects q <= 0 and overfill. |
| `book.OrderBook` | `bids` (`TreeMap`, reverse), `asks`, `orders: id -> OrderNode`, `openOrders: participant -> count`, `changed` (`LinkedHashSet` of touched levels). Public: `add(Order)` (all-or-nothing), `fillBest(side, maxQty) -> Fill`, `cancel(participant, id) -> CancelResult`, `drainLevelUpdates()`, `bestLevel(side)`, `levels(side)` (read-only, best first), `volumeAt`, `levelCount`, `restingOrderCount`, `isResting`, `openOrders(p)`. Package-private test support: `ordersAt`, `checkInvariants()`. |
| `book.PriceLevel` | Doubly linked list with sentinels. Public getters only (`side`, `price`, `totalVolume`, `orderCount`, `isEmpty`); `addOrder`, `headNode`, `fillHead`, `unlink` are package-private (D44). |
| `book.OrderNode` | Package-private node; sentinels only via `sentinel()`. |
| `book.Fill`, `book.LevelUpdate`, `book.CancelResult`, `book.CancelStatus` | Immutable records/enums. `CancelStatus` = `CANCELLED`, `UNKNOWN_ORDER`. |
| `engine.MatchingEngine` | `onNewOrder(NewOrderCommand) -> OrderOutput`, `onCancel(CancelOrderCommand) -> CancelOutput`, `snapshot(depth)`, `lastTradePrice()`, `lastSequenceNumber()`. State: book, client-order-id map, last order id, last sequence number, last trade price. Reads no clock, counts nothing. |
| `engine.Response` | `(orderId, clientOrderId, status, rejectReason, filledQuantity, remainingQuantity, trades)`; the constructor rejects contradictions (D22). |
| `engine.OrderStatus`, `engine.RejectReason` | `FILLED`, `RESTED`, `CANCELLED`, `REJECTED`; `DUPLICATE_CLIENT_ORDER_ID`, `PRICE_OUTSIDE_BAND`, `MAX_OPEN_ORDERS`, `POST_ONLY_WOULD_CROSS`. |
| `engine.Trade`, `engine.Execution` | Trade `(price, quantity, buyOrderId, sellOrderId, aggressorSide, timestamp)`; Execution = a passive fill addressed to the resting order's owner. |
| `engine.EngineOutput` | Sealed: `OrderOutput(seq, ts, response, passiveFills, levelUpdates)`, `CancelOutput(seq, ts, result, levelUpdates)`. |
| `engine.BookSnapshot` | `(sequenceNumber, lastTradePrice, bids, asks)`. |
| `command.Command` | Sealed: `NewOrderCommand(seq, ts, orderId, request)`, `CancelOrderCommand(seq, ts, participant, orderId)`, `SkipCommand(seq, ts, skippedSeq)`. Records, validated in their constructors. |
| `command.Sequencer` | Takes a `java.time.Clock`. `stampNewOrder`, `stampCancel`, `stampSkip`; `resume(clock, lastSeq, lastOrderId, lastTs)`. Backwards clock reuses the previous timestamp. |
| `journal.JournalCodec` | The text format (§4). |
| `journal.CommandJournal` | `open(path, rulesVersion)` (lock the file, verify the header, then cut a torn tail or write a new header), `append`, `appendAll` (one write + one fsync), `read(consumer)` (streaming), fail-stop, `getTruncatedBytesOnOpen`. |
| `core.Replay` | Two passes: verify (contiguous seq, non-decreasing ts, collect SKIPs and counters), then feed. `stopBefore` for the poison rebuild. |
| `core.Exchange` | Single-threaded live path. `open(path, clock)` replays; `process(batch, listener)`; poison-pill quarantine + rebuild; static `quarantine(path, seq, clock)` operator tool; convenience `submit`/`cancel`/`snapshot`. |
| `core.ExchangeService` | The writer thread. `start(path, listener[, settings])`; `submit`/`cancel`/`snapshot` returning futures, and callback variants; `stop(timeout)`; `awaitTermination()`; states `RUNNING`, `STOPPING`, `STOPPED`, `HALTED`; stats `batches()`, `processed()`, `largestBatch()`. `Settings(queueCapacity, maxBatchSize, lingerNanos, clock)`. |
| `core.ExchangeException` | Reasons `BUSY`, `STOPPED`, `HALTED`, `INTERNAL_ERROR`. |
| `core.OutputListener`, `core.CommandInterceptor` | Output hook (writer thread); package-private test seam. |
| `gateway.TcpGateway`, `ClientSession`, `Protocol`, `ClientMessage`, `ProtocolException` | Plain TCP gateway (§6). |
| `client.ExchangeClient`, `client.ClientMain` | Test/interactive client. |
| `Main` | `--port`, `--journal`, `--quarantine <seq>`; exit 0 clean, 1 startup failure, 2 halted. |

**Order types (all tested):**

| Type | Fully fills now | Partly fills now | Nothing crosses |
|---|---|---|---|
| LIMIT | FILLED | RESTED, remainder rests | RESTED |
| IOC | FILLED | CANCELLED, remainder | CANCELLED |
| FOK | FILLED | *(cannot happen)* CANCELLED, all, no trades | CANCELLED |
| MARKET (crosses every level inside the collar) | FILLED | CANCELLED, remainder | CANCELLED |
| POST_ONLY | *(rejected)* | *(rejected)* | RESTED |

Any type can be REJECTED for a duplicate client order id; priced types for `PRICE_OUTSIDE_BAND`; LIMIT and
POST_ONLY for `MAX_OPEN_ORDERS`; POST_ONLY that would cross (equal price included) for `POST_ONLY_WOULD_CROSS`.

**The pipeline:**

```
gateway: parse line -> OrderRequest (stateless) -> ExchangeService.submit (future / callback)
writer thread, per batch: stamp all (Sequencer) -> journal.appendAll (one write, one fsync)
                          -> for each: engine -> complete future (reply) -> OutputListener (fills, market data)
restart: CommandJournal.open (cut torn tail, check header) -> Replay (verify, feed) -> Sequencer.resume
```

**Invariants (all tested; `OrderBook.checkInvariants()` re-derives the book ones from scratch after each of
30,000 random engine commands):**
- `PriceLevel.totalVolume` = sum of remaining over linked nodes; `orderCount` = linked nodes; prev/next walks agree.
- An id is in the id map iff its order rests; per-participant open-order counts match the resting orders.
- No empty level; levels strictly ordered; the book is never crossed after a command.
- `add` is all-or-nothing (a failed add leaves no empty level behind).
- An invalid `Order`, `OrderRequest`, `Command`, `Response`, `Trade` or `CancelResult` cannot be constructed.
- Every trade has exactly one passive fill; for every response, filled = sum of its trades and
  original = filled + remaining.
- A FOK or post-only that does not go ahead leaves the book unchanged.
- Determinism: the same commands produce equal outputs.
- Sequence numbers contiguous from 1, order ids strictly increasing, timestamps non-decreasing, in the journal
  and across restarts.
- Visible ⇒ durable: no reply, fill or market data exists for a command that is not fsynced.
- Restart rebuilds the identical book (snapshot equality, time priority, partial fills, client order ids).
- Replay never writes to the journal; the poison rebuild appends exactly one SKIP.
- At most one open `CommandJournal` per file, across processes (D65); `open` never modifies a file it refuses
  (D64).
- A session that stops reading (QUIT, end of stream, overlong line) closes only after every request it handed in
  has been answered (D63).
- No accepted request is lost: every future completes with a result or a refusal, including when `stop()` races
  producers (tested 5 x 1,000 requests) and when the writer halts.

---

## 3. Decisions (settled)

D1–D43 from Spec v8 still hold unless revised below. Short form:

| # | Decision |
|---|---|
| D1 | `OrderBook` owns the `orderId -> OrderNode` map. |
| D2 | One removal path for fills and cancels (`removeNode` -> `unlink`). |
| D3 | `fillHead` only fills; removal is the book's job. |
| D4 | *Revised by D44:* the book removes an exhausted order inside `fillBest`, not the engine. |
| D5–D9 | `OrderNode` wrapper; own doubly linked list with node handles; sentinels; `unlink` nulls links; `unlink` guards sentinels and foreign nodes. |
| D10, D14, D18 | Eager removal of emptied levels, inside the removal path, checking `TreeMap.remove` returned that level. |
| D11 | *Resolved by D59:* no cached best-level pointer. |
| D12 | Source of truth = sequenced input commands, replayed through the real engine. |
| D13 | State-dependent rejects are journalled. |
| D15, D16 | Duplicate id in `add` throws; ids never reused, gaps allowed (skipped commands leave gaps). |
| D17 | Cancel returns `CancelResult`; `UNKNOWN_ORDER` is a normal outcome. |
| D19 | *Revised by D47.* |
| D20 | Limits are code constants (now in `Rules`); *extended by D46.* |
| D21 | One entry point on one thread: now `ExchangeService` (any thread) -> writer thread -> `Exchange`. |
| D22 | `Response` fields cannot contradict each other (now checked in its constructor). |
| D23 | Four statuses; REJECTED now has four reasons (D49, D50, cap, post-only). |
| D24 | `process`: checks that change nothing -> shared loop -> exhaustive `switch` over `OrderType`. |
| D25, D26 | No default type past the gateway; a market order has no price (`OptionalLong` in the API, `-` in the journal). |
| D27 | `IllegalArgumentException` = bad input; `IllegalStateException` = our bug / corrupt journal. |
| D28 | One definition of "crosses" (`MatchingEngine.crosses(side, limit, restingPrice)`). |
| D29 | Post-only that would cross is rejected, not slid. |
| D30 | "Is this type supported" is stateless. |
| D31 | Commands are immutable data under a sealed interface (now records, plus `SkipCommand`). |
| D32, D33 | The sequencer owns all stamps; backwards clock reuses the previous timestamp (now testable: injected `Clock`). |
| D34 | A trade's timestamp = the incoming command's timestamp. |
| D35 | The engine rejects a non-increasing order id (and now a non-increasing sequence number) before any mutation. |
| D36 | The journal is a plain text file. |
| D37 | *Revised by D52:* fsync per batch. |
| D38 | Recovery cuts a torn last record; a complete unparsable record is never dropped. |
| D39 | Fail-stop journal. |
| D40 | Stamp -> append -> process lives in `Exchange`; replay feeds the engine directly. |
| D41 | Replay restores state through the engine; the sequencer resumes from the journal; replay outputs are discarded. |
| D42 | Replay verifies contiguity and timestamps (now in a first pass, before anything is fed). |
| D43 | `Exchange` owns the journal and closes it if opening fails. Now `Exchange.open` creates everything itself, so no caller holds a reference that could bypass the journal. |

New in ExchangeServer2:

| # | Decision | Reason |
|---|---|---|
| D44 | **Packages enforce ownership.** `PriceLevel` mutators are package-private; the engine reads levels and calls `OrderBook.fillBest`, which fills, removes an exhausted order and an emptied level, and returns an immutable `Fill`. | The compiler, not convention, keeps the engine out of book internals. Closes the v3/v8 `fillHeadOrder` tension. |
| D45 | **Every order carries a participant and a client order id.** Sessions log in as a participant; cancels carry the participant. | Needed for cancel ownership (any client could cancel any id otherwise), routing passive fills to their owner, and duplicate detection (D49). |
| D46 | **The journal's first record is `HEADER,<format>,<rulesVersion>`; `open` refuses a journal written under other rules.** | D20's lesson made mechanical: replaying under changed rules would silently build a different book. Crossing a rule change = snapshot + fresh journal (not built yet). |
| D47 | **Field validation lives only in `OrderRequest`; `Order` can only be built from one** and checks only what the request cannot know (id, timestamp). | The two copies of the same rules in v8 had to be kept in step; now an invalid `Order` is impossible by type, not by duplication. |
| D48 | **Cancelling someone else's order returns `UNKNOWN_ORDER`**, no `NOT_OWNER`. | A distinct status would tell any participant which ids are live; the caller's next action is the same. |
| D49 | **A client order id is unique per participant, forever.** A repeat is REJECTED `DUPLICATE_CLIENT_ORDER_ID` with the **original** order id. Every new order uses up its id, rejected ones included. | Closes the "durable ⇒ visible" gap of v8 §4: a client that got no reply (crash, halt) resends with the same id after restart and learns the original's id instead of creating a duplicate. Cost: the map only grows (open thread). |
| D50 | **Price band and market collar:** priced orders must be within ±10% of the last trade (else `PRICE_OUTSIDE_BAND`); market orders trade only inside the same range, fixed at arrival (`floor`/`ceil` inward, equal to `withinBand`). No band before the first trade. | Fat-finger protection and market-order price protection (v8 open thread). Fixed at arrival so an order's own trades cannot widen its collar. |
| D51 | **Poison-pill policy.** Live: a `RuntimeException` from the engine on command *k* -> append `SKIP k` (fsynced) -> rebuild the engine by replaying everything before *k* -> continue the batch; the caller gets `INTERNAL_ERROR`; the listener never hears of *k*. An `Error` halts instead. At startup a failing record is **never** skipped automatically: startup fails, an operator inspects it and runs `--quarantine <seq>`. | Live, *k*'s effects were never visible (outputs are emitted only after success), so history without *k* is exactly what clients saw. At startup we cannot tell a poison from a record that succeeded under older code; skipping the latter would silently diverge. |
| D52 | **Smart batching:** the writer takes up to `maxBatchSize` (1,000) waiting requests, stamps them, appends them with one write + one fsync, then processes them. `lingerNanos` (group commit with a timeout) exists only for comparison. | Measured (§5): same latency as fsync-per-command at low load, many times the throughput under load. Write-ahead still holds: nothing in a batch is processed before the batch's fsync. |
| D53 | **Wire protocol:** ASCII lines, `TYPE key=value ...` outbound; FILL lines before the ORDER outcome; market data after the reply for the same command. | Self-describing, extensible, debuggable with any line client. Order = "received -> matches -> done" (v8 §4). |
| D54 | **The writer thread never blocks on a client.** Session outbound queues are bounded; a full queue disconnects that client. | One slow client must not stall the exchange for everybody (a classic exchange failure mode). |
| D55 | **The queue carries requests, not stamped commands;** stamping happens on the writer thread. | Sequence order = queue order = processing order, with no locking around the sequencer; producers never read the clock. |
| D56 | **Capacity is enforced under the hand-in lock; the queue itself is unbounded; a full queue refuses with BUSY.** `stop()` flips the state and enqueues STOP under the same lock. | "Refused" and "never lost" at once (threads guide 4.3): nothing can be accepted after STOP, and `stop()` never blocks on a full queue. Refusing instead of blocking keeps gateway threads responsive and makes backpressure visible. |
| D57 | **Reads go through the queue.** `snapshot` is a non-journalled request; `SUBSCRIBE` takes the snapshot and joins the subscribers in one writer-thread step. | No other thread ever touches the book (no data race); a subscriber sees no gap and no duplicate between snapshot and updates. |
| D58 | **Only the writer thread closes the journal, as its last act;** a halted process exits with code 2. | Nothing can be using the journal when it closes (shutdown bug 2); fail-stop at process level. |
| D59 | **No cached best-level pointer.** | Measured: `TreeMap.firstEntry` costs 8–18 ns at 10–100,000 levels, under 2% of a ~1 µs command. |
| D60 | **Replay streams the file twice** (verify, then feed) instead of loading a `List`. | Constant memory, and nothing is fed if the history is invalid anywhere. The poison-pill rebuild uses the feed pass alone with the skip set `Exchange` already holds (the journal was verified at open and only the writer has appended since). |
| D61 | **Callers never get the writer's own future.** `submit`/`cancel`/`snapshot` return a separate future, completed from the common pool; the writer decides what to process from its own state (`Pending.refused`), never from a future's state. The callback variants (gateway only) still run on the writer and must not block. | Found in review: with the shared future, a caller's `cancel()`/`orTimeout()` on a queued request made the writer skip a command it had already journalled (live book ≠ replayed book), and any continuation a caller attached ran on, and could block, the writer thread. |
| D62 | **SUBSCRIBE snapshots at most `Protocol.SNAPSHOT_DEPTH` (100) levels per side.** | Bounds the writer-thread cost and the line length of a SUBSCRIBE whatever the book depth (a client could loop SUBSCRIBE/UNSUBSCRIBE). LEVEL updates carry absolute quantities, so a deeper level appears in a subscriber's view when it first changes. |
| D63 | **A session closes only after its requests are answered.** When the reader stops (QUIT, end of stream, overlong line) it queues a depth-1 snapshot read; the read's callback sends BYE (for QUIT) and closes. A broken connection (I/O error) still closes at once. | The queue is FIFO, so that read completes after every request the session handed in, and their reply callbacks have already offered their lines. Before, QUIT sent BYE and closed at once: a client sending NEW then QUIT lost the ORDER reply (199 of 200 in the regression test) for an order that executed and may rest. A read reuses the existing request type instead of a new "barrier" request in `core`; it costs the writer one tiny snapshot per disconnect. During shutdown the read is refused at once and the session closes without waiting (shutdown drops connections anyway). |
| D64 | **`CommandJournal.open` checks the header before it changes anything.** A complete first record must be a header of this rules version; a file with no complete record must be a prefix of the expected header (a crash while creating the journal). Otherwise `open` refuses and the file is untouched. | Before, the torn-tail cut ran first: pointing `--journal` at any other file cut its unterminated last line, and a one-line file without a newline was emptied and turned into a journal. A journal written under other rules was also cut before being refused. |
| D65 | **A journal is locked while open, through a separate `<journal>.lock` file.** `open` first adds the lock file's real path to an in-JVM set (`HELD`), then takes an exclusive `FileLock` on the lock file through a channel nothing else opens; a second open (same JVM or another process) is refused with "already open". `close` closes the journal channel, then the lock channel, then leaves `HELD`. The lock file stays (empty; gitignored). | Nothing stopped `--quarantine` from appending next to a running exchange, interleaving sequence numbers (the next startup fails on the gap/repeat). **Why a separate file:** the first implementation locked one byte at `Long.MAX_VALUE - 1` of the journal itself (past the data, because Windows locks are mandatory). On Linux/macOS Java's locks are POSIX locks and closing *any* handle a process has on a file drops all its locks on it; `open`, `read()` and the torn-tail cut open and close handles on the journal, so that lock would vanish at once (found while writing the study guides; it passed on Windows, where locks are per handle). The in-JVM set refuses a second open before it touches the lock file for the same reason. Tested across processes on Windows (`CommandJournalTest.anotherProcessIsRefusedWhileTheJournalIsOpen`, a child JVM); **not yet run on Linux/macOS**. |

---

## 4. Journal format

```
HEADER,<formatVersion=2>,<rulesVersion>
NEW,<seq>,<timestamp>,<orderId>,<participant>,<clientOrderId>,<side>,<type>,<quantity>,<price or ->
CANCEL,<seq>,<timestamp>,<participant>,<targetOrderId>
SKIP,<seq>,<timestamp>,<skippedSeq>
```

Every record ends with `\n`. Timestamps are `Instant.toString()`. A SKIP has its own sequence number; the
skipped record stays in the file (history is annotated, never edited).

**Failure model:**
- *Visible ⇒ durable* holds (write-ahead, per batch).
- *Durable ⇒ visible* does not: a crash or halt after the fsync and before the reply leaves journalled commands
  whose clients heard nothing (for a halt, every command in the failing batch). They are replayed on restart.
  Clients reconcile by resending with the same client order id (D49).
- A torn last record is cut on open; a complete corrupt record stops startup with its record number. The header is
  verified first, so a file that is not a journal of these rules is refused untouched (D64).
- One process at a time: `open` locks `<journal>.lock` (D65); `--quarantine` against a running exchange is refused.
- A journal I/O failure fail-stops the journal; the writer halts; the process exits 2.

---

## 5. Measurements (roadmap step 6)

Laptop, Windows 11, JDK 17, JMH 1.37, 1 fork. Numbers are indicative (error bars on a laptop are wide).

**Engine** (`MatchingBenchmark`, ns/op, two commands per op for the last two):

| levels per side | `bestLevelLookup` | `restAndTake` | `addAndCancel` |
|---|---|---|---|
| 10 | 7.9 | 2,131 | 1,334 |
| 1,000 | 15.8 | 2,234 | 1,297 |
| 100,000 | 18.5 | 2,169 | 1,534 |

Reading: a command costs ~0.65–1.1 µs, flat in book depth; the O(log L) lookup is visible (8 -> 18 ns) but is
under 2% of a command. The cost is allocation (records, lists, `Optional`, strings in the benchmark itself) and
the client-order-id map, not the tree. Decision D59.

**Batching** (`BatchingBenchmark`, closed-loop clients, journal fsynced on the project disk):

Each client submits one order and waits for its reply before the next. One fsync costs ~1.5 ms on this disk.

| clients | mode | throughput (orders/ms) | latency p50 | p99 | avg batch |
|---|---|---|---|---|---|
| 1 | per command (maxBatch 1) | 0.76 | 1.5 ms | 34 ms | 1 |
| 1 | linger 1 ms | 0.07 | 16 ms | 45 ms | 1 |
| 1 | smart | ~0.6 (noisy) | 1.6 ms | 2.4 ms | 1 |
| 16 | per command | 0.83 | 19 ms | 31 ms | 1 |
| 16 | linger 1 ms | 1.1 | 16 ms | 25 ms | 16 |
| 16 | smart | 11.0 | 1.2 ms | 2.7 ms | 15 |
| 64 | per command | 0.81 | 152 ms | 186 ms | 1 |
| 64 | linger 1 ms | 4.1 | 20 ms | 38 ms | 64 |
| 64 | smart | 43.3 | 1.4 ms | 2.4 ms | 63 |

Reading:
- **Per command** is capped at ~1 order per fsync whatever the load; latency grows with the queue (Little's law:
  64 waiting clients x 1.5 ms ≈ 100+ ms).
- **Smart batching** has the same single-client latency as per command (a batch of one), and its throughput
  grows with the load (batch ≈ number of waiting clients): 53x per command at 64 clients, with p50 still one fsync.
- **Linger** is worse than both here. Its "1 ms" timeout actually waits ~16 ms: Windows' default timer resolution
  is ~15.6 ms, so every timed wait rounds up. A timer-based batch pays that on every batch; smart batching has no
  timer. (On Linux the linger would be closer to 1 ms, but it would still add that 1 ms to every lightly loaded
  request.)

Decision D52 (smart batching, default `maxBatchSize` 1,000) stands on these numbers.

---

## 6. Gateway

Plain TCP, `TcpGateway(port)`; protocol documented in `Protocol.java` and the README. Thread-per-connection: one
reader (parse, stateless checks, hand-in) and one sender (bounded queue -> socket) per session, plus one acceptor.
Max 256 sessions, 10,000 queued lines per session, 256-byte lines (longer => ERROR + disconnect). Routing: reply ->
requesting session; passive fill -> every session of the owner; TRADE/LEVEL -> subscribers. LOGIN has no
authentication (non-goal). QUIT and end of stream close the session only after its earlier requests are answered
(D63); resting orders stay on the book after a disconnect (no cancel-on-disconnect).

---

## 7. Mutation testing

`mvnw -Pmutation test` (PIT 1.17, packages `domain`, `book`, `engine`, `command`, `journal`, `core`; the gateway
is covered by end-to-end tests, not mutated).

| run | mutations | killed | test strength | line coverage |
|---|---|---|---|---|
| first | 476 | 432 (91%) | 94% | 94% |
| after fixes | 478 | 444 (93%) | 95% | 95% |
| after survey fixes (§10) | 496 | 454 (92%) | 94% | 94% |

The first run found real gaps, now closed by tests: journal close on failed open / service stop / `Exchange.close`
(the delete-based checks proved nothing, see §1.5); `stop()` returning false on timeout; a halt refusing
requests still in the queue (not just in the batch); extra fields in CANCEL/SKIP records; quarantining a SKIP;
the record number in the "no end of record" error; `hasFailed()` false case.

The 42 left after the survey fixes (27 survived, 15 without coverage), by kind:
- **Not observable by a unit test (by design):** removed `force()` (fsync) after the header, after the
  torn-tail cut and in `appendAll`, as in Spec v8; removed log calls; removed `addSuppressed`;
  `awaitTermination()` (used by `Main`); removed `channel.close()` of the journal channel on the failed-open path
  and its `channel != null` guard (the lock is released separately, so a leaked journal channel blocks nothing;
  the counter counts only successful opens). Removing the lock *release* on that path is killed
  (`CommandJournalTest.otherRulesVersionIsRefusedAndTheFileIsReleased` reopens).
- **Covered only in a child JVM (D65):** the branch of `Lock.acquire` where another process holds the lock
  (`tryLock()` returns null) and its cleanup run only in `anotherProcessIsRefusedWhileTheJournalIsOpen`'s child
  process, which PIT does not instrument: 3 mutants without coverage.
- **Equivalent:** `setDaemon(false)` on a thread that is already non-daemon; `<=` vs `<` where a second layer
  throws anyway (`OrderBook.add`, `PriceLevel.fillHead`, overflow guard); linger and scan-loop boundaries that
  only change an extra iteration; `StringBuilder` capacity; header-length cap.
- **Test support / cosmetic:** boundaries in `OrderBook.checkInvariants` (test-only); `toString`; `hashCode`
  returning 0 (still correct, only slower); `getFile`; the callback-less `cancel` future variant.

---

## 8. Open threads

**Known limitations (accepted for now)**
- **Client-order-id map only grows** (D49). Fix: scope ids to a trading day; end of day = snapshot + fresh journal.
- **No snapshots:** recovery replays the whole journal (O(file size), streamed). Snapshots + rotation are the
  first "next candidate" (Vision §7).
- **No output sequence numbers / resend:** a passive fill for a participant with no open session is not stored;
  a reconnecting client cannot recover missed fills or market data (re-SUBSCRIBE gives a fresh snapshot).
- **Halted batch:** commands of the failing batch are journalled but their callers are told HALTED; they replay on
  restart (durable ⇒ visible gap, D49 is the client-side remedy).
- **Rules version** catches deliberate rule changes, not accidental behaviour changes in code; a code change that
  alters matching still needs snapshot + fresh journal by discipline.
- **Open-order cap is checked on arrival**, so a LIMIT that would fully fill is rejected when the participant is
  at the cap (simple and deterministic; real venues differ).
- **Self-trades are allowed** (no self-trade prevention).
- **Journal:** directory not fsynced after creating the file (matters on Linux); no per-record checksum. Records
  end in LF only: a tool that converts the file to CRLF (an editor, git `autocrlf`) makes it unreadable (startup
  fails on the header). `--quarantine` accepts a command that is already skipped (a harmless second SKIP).
- **Startup reporting:** `Main` prints neither how many commands were replayed nor how many torn bytes were cut
  (`Exchange.replayedOnOpen()` / `truncatedBytesOnOpen()` exist but only tests read them).
- **Gateway:** two platform threads per connection; no authentication or TLS; market data uses the command
  sequence number, so gaps are normal (commands without public effects).
- `ExchangeService.stop()` called from the writer thread itself (e.g. inside a listener) would wait for its own
  end until the timeout.

- The gateway acceptor survives transient `accept()` failures by logging and backing off 100 ms; it has no alarm
  if failures persist (observability is a next candidate).

**Resolved since Spec v8:** O(1) cancel kept; single-writer enforcement (D43, D55); injectable clock; RESTED
snapshot (passive fills, D45); market order price protection (D50); sentinel out of the client API and the file
format; participant id; `fillHeadOrder` tension (D44); test move to `src/test/java` and one file per class;
build file; batching decided (D52); cached pointer decided (D59); poison-pill policy (D51); `jdk.jfr.Description`
misuse, empty `Main`, duplicated `.gitignore`, test-only public `submit(Order)` all gone.

---

## 9. Code review (2026-10-27)

A `/code-review` of the whole source tree reported 10 findings; all are fixed. Each fix that a test can observe
has a regression test, and each of those tests was checked by putting the old code back: it fails.

| # | Finding | Fix | Regression test |
|---|---|---|---|
| 1 | Caller cancelling / timing out its future made the writer skip a journalled command | D61 (own future, `Pending.refused`) | `ExchangeServiceTest.cancellingTheCallersFutureDoesNotStopAJournalledCommand` |
| 2 | Acceptor thread died silently on any `SocketException` (e.g. too many open files) | Exit only if the server socket is closed; otherwise log and back off | (not unit-testable; reviewed) |
| 3 | UNSUBSCRIBE overtaking the snapshot left the client's last word SUBSCRIBED while unsubscribed; S/U/S could send two BOOKs | Subscription state under one per-session lock, numbered subscriptions | `TcpGatewayTest.rapidSubscribeUnsubscribeEndsConsistent` |
| 4 | A session closed during SUBSCRIBE or LOGIN was re-registered after `removed()` and leaked | Add, then re-check `closed` and undo | (race; reviewed, add-then-check vs set-then-remove) |
| 5 | Socket leaked when the "too many sessions" write or `setTcpNoDelay` failed | Close in the catch | (reviewed) |
| 6 | `sessionsByParticipant` kept an empty entry per name ever used | Add/remove via `compute`/`computeIfPresent`, dropping empty sets atomically | `TcpGatewayTest.participantRegistryForgetsDisconnectedParticipants` |
| 7 | SUBSCRIBE snapshotted the whole book on the writer thread | D62 | `TcpGatewayTest.snapshotIsBoundedToTheSnapshotDepth` |
| 8 | Caller continuations ran on, and could block, the writer thread | D61 | `ExchangeServiceTest.callerContinuationsRunOffTheWriterAndCannotStallIt` |
| 9 | Poison rebuild read the journal twice | `Replay.feed` with the known skip set (D60) | `ReplayTest.feedAloneUsesTheGivenSkipSetAndDoesNotVerify` |
| 10 | The `\r` of `\r\n` counted toward the 256-byte line limit | Limit applies to the content | `ClientSessionTest.ReadLine` |

---

## 10. Fixes from the study-guide survey (2026-10-27)

Found while surveying the code to write `docs/understanding/`. Each regression test was run against the old code
(restored from git): all seven fail there and pass now.

| # | Finding | Fix | Regression test |
|---|---|---|---|
| 1 | Flaky: `waitingRequestsShareOneBatch` failed once in a full run (`expected 50 but was 1`). The writer updates `batches`/`processed`/`largestBatch` after `process()` has completed the batch's futures, so a test woken by the last future can read the previous values. Five assertions had the race. | Tests read the stats after `stop()` (the writer's `join` is the happens-before edge); the field comment says the stats lag the replies. Production behaviour unchanged. | the five `ExchangeServiceTest` assertions themselves |
| 2 | QUIT (or end of stream) right after NEW dropped the reply of an order that executed | D63 | `TcpGatewayTest.quitAnswersEveryEarlierRequestBeforeBye`, `endOfStreamStillGetsTheReplies` |
| 3 | `open` cut the last line of any file before checking its header; a one-line file was overwritten | D64 | `CommandJournalTest.fileThatIsNotAJournalIsRefusedUnchanged`, `textWithoutAnyRecordIsRefusedUnlessItStartsAHeader`, `otherRulesVersionWithATornTailIsRefusedUnchanged` |
| 4 | `--quarantine` could append to a journal a running exchange was writing | D65 (lock file; the first version, a byte lock on the journal itself, would not hold on Linux/macOS); `Main --quarantine` reports a refusal in one line (exit 1) instead of a stack trace | `CommandJournalTest.aSecondOpenIsRefusedUntilTheFirstCloses`, `anotherProcessIsRefusedWhileTheJournalIsOpen` (child JVM), `ExchangeTest.quarantineToolRefusesWhileTheExchangeIsRunning`; also checked by hand against a running server |
| 5 | Comments cite a "threads guide" and "Spec v8" that are not in this repo | §1.6 names the files | — |
| 6 | `CLAUDE.md` said decisions run D1–D60, the test count was stale, and it said the project is not a git repository | Updated | — |
