# 13 — Glossary

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** every domain term, project-specific name and abbreviation, with a short definition and where it lives in the code | **Prerequisites:** none (use it alongside any guide) | **Related:** 01, 06, 07 (all modules), 09
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

Alphabetical, grouped by first letter. Paths are relative to `src/main/java/exchangeserver/` unless they start with `src/`, `docs/` or `pom.xml`. Every location was read in the current code **[Verified]** unless marked **[Inferred]**. Where the project's own glossary (Vision §9) defines a term, the definition agrees with it.

Abbreviations used across the guides: **D-n** = design decision n in the Spec; **WAL** = write-ahead log; **L2** = level-2 market data; **IOC/FOK** = immediate-or-cancel / fill-or-kill; **clOrdId** = client order id; **seq** = sequence number; **JMH/PIT** = benchmark / mutation tools.

---

## A

- **Acceptor** — the daemon thread `gateway-acceptor` that accepts TCP connections and creates a `ClientSession` for each, refusing beyond 256 sessions. `gateway/TcpGateway.java:88-124`.
- **Aggressor (taker, incoming order)** — the order being matched *now*; it may never rest. Its side is recorded on every trade as `aggressorSide`. `engine/Trade.java:15`; set at `engine/MatchingEngine.java:148`.
- **`answerFirst` / close after replies** — the session's reader, when it stops on QUIT, end of stream or an overlong line, closes only after a queued depth-1 snapshot read completes, so every earlier reply is out first (D63). `gateway/ClientSession.java:114-153`.
- **Append / `appendAll`** — write a batch of encoded commands with one write and one fsync; sets the journal's fail-stop flag on error. `journal/CommandJournal.java:113-137`.
- **Ask** — a sell order resting on the book; the ask side is a `TreeMap` sorted lowest price first. `book/OrderBook.java:41`.

## B

- **Backpressure / BUSY** — refusing a request when the writer's queue holds `queueCapacity` (default 100,000) requests, instead of queueing without limit. The caller gets `ExchangeException` with reason `BUSY`; nothing is journalled. `core/ExchangeService.java:182-202`, `core/ExchangeException.java:18-23`.
- **Band (price band)** — a priced order must be within ±`PRICE_BAND_PERCENT` (10%) of the last trade price, else REJECTED `PRICE_OUTSIDE_BAND`; no band before the first trade. `domain/Rules.java:29-43`, checked at `engine/MatchingEngine.java:115-117` (D50).
- **Batch** — the requests the writer takes from the queue in one go (up to `maxBatchSize`, default 1,000), stamped, journalled with one fsync, then processed. `core/ExchangeService.java:246-265`, `core/Exchange.java:118-166`.
- **Bid** — a buy order resting on the book; the bid side is a `TreeMap` in reverse order (highest price first). `book/OrderBook.java:40`.
- **BOOK line** — the snapshot a subscriber receives: `BOOK seq= last= bids=<p>x<q>,... asks=...`. `gateway/Protocol.java:160-177`.
- **`BookSnapshot`** — `(sequenceNumber, lastTradePrice, bids, asks)` as of the last processed command; levels best first. `engine/BookSnapshot.java:14-20`.

## C

- **Callback variant** — `ExchangeService.submit/cancel/snapshot(…, BiConsumer)`: the callback is attached before queueing and runs on the writer thread, before the command's listener call; used by the gateway. Must not block. `core/ExchangeService.java:166-179`.
- **Cancel** — removing a resting order by its owner; someone else's order or a gone order yields `UNKNOWN_ORDER` (D48). `book/OrderBook.java:96-107`, `engine/MatchingEngine.java:77-83`.
- **`CancelResult` / `CancelStatus`** — `CANCELLED` (with the cancelled quantity) or `UNKNOWN_ORDER`. `book/CancelResult.java:6-25`, `book/CancelStatus.java:9-12`.
- **`canRest()`** — true only for `LIMIT` and `POST_ONLY`; only those types can sit on the book. `domain/OrderType.java:19-22`.
- **`checkInvariants()`** — package-private test seam that re-derives every book invariant from scratch. `book/OrderBook.java:192-234`.
- **clOrdId (client order id)** — the client's own id for a new order, unique per participant forever; a repeat is REJECTED `DUPLICATE_CLIENT_ORDER_ID` with the original order id (D49). Pattern `[A-Za-z0-9_.-]{1,32}`. `domain/Rules.java:27`, map at `engine/MatchingEngine.java:43`.
- **`ClientMessage`** — sealed interface of parsed client lines (Login, NewOrder, Cancel, Subscribe, Unsubscribe, Ping, Quit); syntax only. `gateway/ClientMessage.java:9-32`.
- **`ClientSession`** — one TCP connection with a reader thread and a sender thread and a bounded outbound queue. `gateway/ClientSession.java`.
- **Collar (market collar)** — the price range a MARKET order may trade inside: the band around the last trade, fixed at arrival, floor/ceil inward. `engine/MatchingEngine.java:180-193` (D50).
- **`Command`** — a stamped, journalled input; sealed: `NewOrderCommand`, `CancelOrderCommand`, `SkipCommand`. The source of truth (D12, D31). `command/Command.java:11-16`.
- **`CommandInterceptor`** — package-private test seam called just before the engine, live and in replay; tests throw (poison) or block (Gate) in it. Production uses `NONE`. `core/CommandInterceptor.java:11-17`.
- **`CommandJournal`** — the write-ahead log file: lock, header check, torn-tail cut, append+fsync, streaming read, fail-stop. `journal/CommandJournal.java`.
- **Crosses / crossing** — an incoming buy crosses an ask at or below its limit; a sell crosses a bid at or above it. The single definition (D28): `engine/MatchingEngine.java:195-199`. A **crossed book** (best bid ≥ best ask) must never exist after a command (`book/OrderBook.java:231-233`).

## D

- **D-number (D1–D65)** — a numbered design decision with its reason in `docs/ExchangeServer2_Spec.md` §3; code comments cite them. Next free: D66.
- **Determinism** — the engine's output depends only on the ordered commands: no clock, counters, randomness or hash-iteration order. Required for replay. `engine/MatchingEngine.java:25-26`; tested by `src/test/java/exchangeserver/engine/MatchingEngineTest.java:605`.
- **`drainLevelUpdates()`** — returns one `LevelUpdate` per level touched since the last call, in first-touched order, and clears the set. `book/OrderBook.java:129-143`.
- **Durable ⇒ visible / visible ⇒ durable** — the second holds (nothing is seen before it is fsynced); the first does not (a crash after fsync and before the reply leaves journalled commands nobody heard about). Spec §4.

## E

- **`EngineOutput`** — sealed: `OrderOutput` (response, passive fills, level updates) or `CancelOutput`; everything one command produced. `engine/EngineOutput.java:14-25`.
- **`Exchange`** — the single-threaded live path: stamp → journal → engine, plus poison-pill quarantine and the static `--quarantine` tool. `core/Exchange.java:49`.
- **`ExchangeException`** — refusal with a reason: `BUSY`, `STOPPED`, `HALTED`, `INTERNAL_ERROR`. `core/ExchangeException.java:16-42`.
- **`ExchangeService`** — owns the writer thread and the queue; any thread hands in requests. `core/ExchangeService.java:50`.
- **`exchange-writer`** — the name of the single writer thread. `core/ExchangeService.java:112`.
- **Execution (passive fill)** — a fill on a resting order, addressed to its owner; sent as `FILL … liquidity=MAKER`. `engine/Execution.java:11-12`.

## F

- **Fail-stop** — after a failure the system stops rather than limping: the journal refuses every append after a failed write/fsync; the writer halts; `Main` exits 2. `journal/CommandJournal.java:115-117,132-136`, `Main.java:85-90` (D39, D58).
- **Fill** — (1) `book.Fill`: immutable record of what `fillBest` did to one resting order (`book/Fill.java:11-12`); (2) the `FILL` wire line, sent to both taker and maker (`gateway/Protocol.java:131-146`). Compare **Trade** (the public fact) and **Execution** (the maker's private fill).
- **`fillBest`** — the book fills the head of the best level by up to a quantity, removes an exhausted order and an emptied level, returns a `Fill` (D44). `book/OrderBook.java:75-94`.
- **FOK (fill-or-kill)** — fill completely now or do nothing; a pre-check walks crossing levels without mutating. `engine/MatchingEngine.java:124-127,201-214`.
- **Format version** — `JournalCodec.FORMAT_VERSION` (2): the record layout version, in the header. `journal/JournalCodec.java:34`.
- **fsync / `force(true)`** — forcing written bytes (and metadata) to the disk; ~1.5 ms on the dev disk. `journal/CommandJournal.java:131`.
- **Future (caller's)** — each `submit/cancel/snapshot` returns its own `CompletableFuture`, completed on the common pool from the writer's internal one (`handOff`, D61). `core/ExchangeService.java:142-163`.

## G

- **Gate** — test helper implementing `CommandInterceptor` that blocks the writer on orders whose clOrdId starts with `HOLD`. `src/test/java/exchangeserver/core/ExchangeServiceTest.java` (class `Gate`).
- **Gateway** — the TCP front end: `TcpGateway` + `ClientSession` + `Protocol`. `gateway/`.
- **Group commit / linger** — collect requests until a batch is full or `lingerNanos` elapse, then one fsync. Kept only for benchmark comparison; default 0 (off). `core/ExchangeService.java:285-299` (D52).

## H

- **`handOff`** — wraps the writer's internal future into a fresh one completed via `whenCompleteAsync`, so callers cannot block the writer or cancel its future (D61). `core/ExchangeService.java:154-164`.
- **Halt / HALTED** — the writer hit an unrecoverable failure (journal I/O, an `Error`, interrupt); every pending request is refused `HALTED`; the journal closes; `Main` exits 2. `core/ExchangeService.java:302-319`.
- **Happens-before** — the Java Memory Model guarantee that one action's effects are visible to another (e.g. `queue.put` → `take`, future `complete` → `get`, `Thread.start`, `Thread.join`). Why `Pending` fields need no `volatile`. `core/Pending.java:12-13`.
- **Header** — the journal's first record, `HEADER,<format>,<rules>`; checked before anything is changed (D46, D64). `journal/JournalCodec.java:46-66`, `journal/CommandJournal.java:256-269`.

## I

- **Id map** — `orders: orderId → OrderNode`; contains an id iff the order rests (D1). `book/OrderBook.java:42`.
- **Invariant** — a property that must always hold (e.g. level volume = sum of remaining; no empty level). Listed in Spec §2 and `book/OrderBook.java:22-27`, `book/PriceLevel.java:20-25`.
- **IOC (immediate-or-cancel)** — match what crosses now, cancel the remainder. `domain/OrderType.java:6`.

## J

- **JMH (Java Microbenchmark Harness)** — benchmark framework; benchmarks in `src/test/java/exchangeserver/bench/`, run with `-Pbench`. `pom.xml:93-125`.
- **Journal (WAL, write-ahead log)** — the text file of stamped commands; persisted before processing. Format: `journal/JournalCodec.java:16-30`.
- **`JournalCodec`** — encodes/decodes records; decoding rebuilds requests through `OrderRequest` so the same stateless rules apply. `journal/JournalCodec.java:32`.

## L

- **L2 (level-2) market data** — per-price aggregate volume and order count (not individual orders); here `LEVEL` lines and `LevelUpdate`. `book/LevelUpdate.java:7-11`.
- **Last trade price** — the band/collar reference; 0 means no trade yet. `engine/MatchingEngine.java:46`.
- **Leaves** — quantity still open on an order after a fill. `book/Fill.java:12`, `Protocol.takerFills` counts it down (`gateway/Protocol.java:130-137`).
- **Level (price level, `PriceLevel`)** — all orders at one price on one side, in time order, as a doubly linked list with sentinels. `book/PriceLevel.java:28`.
- **`LevelUpdate`** — `(side, price, volume, orderCount)` after a command; `volume == 0` means the level is gone. `book/LevelUpdate.java:10`.
- **LIMIT** — match what crosses now, rest the remainder. `domain/OrderType.java:5`.
- **Listener (`OutputListener`)** — receives every command's output on the writer thread after the reply; the gateway implements it for maker fills and market data. Not called in replay. `core/OutputListener.java:14-20`, `gateway/TcpGateway.java:182-207`.
- **Lock (journal lock, `<journal>.lock`)** — an exclusive whole-file `FileLock` on a separate, empty sibling file (e.g. `exchange.journal.lock`) that nothing else ever opens, taken before the journal is opened and released after it closes. A static in-JVM `HELD` set refuses a second open in the same JVM before the lock file is touched. A second open anywhere is refused "already open" (D65). The lock file stays on disk (git-ignored `*.journal.lock`). `journal/CommandJournal.java:52-53,198-249`.

## M

- **Maker / passive / resting** — an order sitting on the book; when hit, its owner receives `FILL … liquidity=MAKER`. Vision §9.
- **MARKET** — no price; trades at any price inside the collar; remainder cancelled. `domain/OrderType.java:8`.
- **`MatchingEngine`** — price-time matching, risk checks, outputs; deterministic. `engine/MatchingEngine.java:37`.
- **`MutableClock`** — test clock that can be set or advanced, including backwards. `src/test/java/exchangeserver/testing/MutableClock.java`.
- **Mutation testing (PIT)** — PIT changes the bytecode (e.g. flips `<` to `<=`) and checks a test fails ("killed"); survivors show untested behaviour. `pom.xml:126-169`, results in Spec §7.

## O

- **Open orders / open-order cap** — resting orders per participant (`openOrders` map), capped at 10,000 for resting types (`MAX_OPEN_ORDERS`). `book/OrderBook.java:43`, `engine/MatchingEngine.java:118-121`, `domain/Rules.java:33`.
- **`Order`** — an accepted order: a valid request plus id and timestamp; tracks filled quantity. `domain/Order.java:16`.
- **Order id** — exchange-issued, from the sequencer, strictly increasing; gaps allowed. `command/Sequencer.java:48-52`.
- **`OrderNode`** — package-private linked-list node; `order == null` ⇔ sentinel. `book/OrderNode.java:18-35`.
- **`OrderRequest`** — a client order before stamping; the only field-validation layer (D47). `domain/OrderRequest.java:15`.
- **`OrderStatus`** — `FILLED`, `RESTED`, `CANCELLED`, `REJECTED`. `engine/OrderStatus.java:4-9`.

## P

- **Participant** — the logged-in identity owning orders; pattern `[A-Za-z0-9_]{1,16}`; no authentication. `domain/Rules.java:26`, `gateway/ClientSession.java:225-243`.
- **`Pending`** — a request in flight with its own future, its stamped `command`, and the writer-owned `refused` flag. `core/Pending.java:16-36`.
- **Poison pill** — a journalled command that makes the engine throw, and so would crash every replay. `core/Exchange.java:41-44` (D51).
- **POST_ONLY** — rests without matching; REJECTED `POST_ONLY_WOULD_CROSS` if it would cross on arrival (equal price counts). `engine/MatchingEngine.java:128-133`.
- **Price-time priority** — best price first; at one price, earliest arrival first (FIFO within a level). `book/PriceLevel.java:63-87`.
- **`Protocol`** — the wire protocol: parsing and line formatting; its header comment is the protocol documentation. `gateway/Protocol.java:16-40`.

## Q

- **Quarantine** — appending `SKIP <seq>` so replay passes over a poison pill without editing history; automatic live (`core/Exchange.java:178-189`), operator-run at startup via `--quarantine <seq>` (`core/Exchange.java:94-114`, `Main.java:47-58`).
- **Queue** — the single `LinkedBlockingQueue<Pending<?>>` into the writer; unbounded, capacity enforced under a lock (D56). `core/ExchangeService.java:95`.

## R

- **Refused (`p.refused`)** — set by the writer when stamping rejects a request (e.g. invalid cancel); such a request is never journalled and is skipped in processing, decided from writer state, not the future (D61). `core/Exchange.java:127-131,142-144`.
- **`RejectReason`** — state-dependent rejects: `DUPLICATE_CLIENT_ORDER_ID`, `PRICE_OUTSIDE_BAND`, `MAX_OPEN_ORDERS`, `POST_ONLY_WOULD_CROSS`. `engine/RejectReason.java:9-18`.
- **`removeNode`** — the single removal path for fills and cancels (D2). `book/OrderBook.java:109-127`.
- **Replay** — rebuilding engine state by feeding journalled commands through the same engine: pass 1 verifies, pass 2 feeds (D60). `core/Replay.java:28-90`.
- **`Request`** — sealed: `Submit`, `Cancel`, `Snapshot`; what callers hand in before stamping (D55). `core/Request.java:16-34`.
- **`Response`** — what a new order's submitter is told; its constructor rejects contradictory fields (D22). `engine/Response.java:15-54`.
- **Rules version** — `Rules.VERSION` (1); recorded in the header; a mismatch refuses startup. `domain/Rules.java:19`.

## S

- **Sentinel** — dummy head/tail nodes of a level's list, removing edge-case branches. `book/PriceLevel.java:31-32`, `book/OrderNode.java:32-34`.
- **Sequence number (seq)** — contiguous number of every journalled command (new, cancel, skip), from 1. `command/Sequencer.java:13`. Market-data lines carry it, so gaps are normal.
- **`Sequencer`** — the only component that reads the clock or counts; backwards clock reuses the previous timestamp (D32, D33). `command/Sequencer.java:20-82`.
- **`Settings`** — `queueCapacity`, `maxBatchSize`, `lingerNanos`, `clock`. `core/ExchangeService.java:59-87`.
- **Single writer** — exactly one thread (`exchange-writer`) touches sequencer, journal, engine and book; no locks inside them. `core/ExchangeService.java:22-49`.
- **SKIP record / `SkipCommand`** — `SKIP,<seq>,<ts>,<skippedSeq>`; can only skip an earlier command. `command/SkipCommand.java:14-25`.
- **Slow consumer** — a session whose bounded outbound queue (10,000 lines) is full; it is disconnected rather than waited for (D54). `gateway/ClientSession.java:77-86`.
- **Smart batching** — take whatever is already queued, one fsync, process; no timer. The default (D52). `core/ExchangeService.java:252-254`.
- **Snapshot** — a read through the queue returning `BookSnapshot` (bounded to 100 levels per side for SUBSCRIBE, D62); never journalled. `core/Request.java:24-33`, `engine/MatchingEngine.java:85-91`.
- **Spec / Vision** — `docs/ExchangeServer2_Spec.md` (exact code state, D-numbers, journal format, measurements, open threads) and `docs/ExchangeServer2_Vision.md` (purpose, working style, principles, roadmap).
- **States (RUNNING, STOPPING, STOPPED, HALTED)** — `ExchangeService` lifecycle. `core/ExchangeService.java:35-57`.
- **STOP marker** — a private `Pending` compared by identity; enqueued by `stop()` under the lock so every accepted request is ahead of it. `core/ExchangeService.java:91,208-217`.
- **Subscription** — a session receiving TRADE/LEVEL lines after `SUBSCRIBE`; numbered and guarded by a per-session lock against SUBSCRIBE/UNSUBSCRIBE races. `gateway/ClientSession.java:266-314`.

## T

- **Taker** — see Aggressor; `FILL … liquidity=TAKER`.
- **Time priority** — within a level, the earliest order fills first (tail append, head fill). `book/PriceLevel.java:63`, `:94-113`.
- **Torn tail** — bytes after the last `\n` (a record cut off by a crash); cut on open after the header check. `journal/CommandJournal.java:271-306`.
- **Touch** — the best bid and best ask levels. Vision §9.
- **Trade** — a public match: `(price, quantity, buyOrderId, sellOrderId, aggressorSide, timestamp)`; price is always the resting level's; timestamp is the incoming command's (D34). `engine/Trade.java:8-27`.

## U

- **`UNKNOWN_ORDER`** — cancel outcome for a missing, already-gone or not-owned order. `book/CancelStatus.java:3-8`.
- **Unlink** — O(1) splice of a node out of its level's list; nulls the node's links. `book/PriceLevel.java:115-134`.

## W

- **`withinBand`** — exact integer check `price*100` vs `ref*(100±band)`. `domain/Rules.java:38-43`.
- **Write-ahead** — persist (fsync) a command before processing it; "nothing is processed before its batch is fsynced". `core/Exchange.java:137`.

---

## Key takeaways

- Three different "fill" words: **Trade** (public, one per match), **Execution** (the maker's private fill), **Fill** (the book's internal record, and the wire line for both sides).
- "Order id" is exchange-issued and increasing; "client order id" is the client's and unique per participant forever.
- "Sequence number" counts every journalled command; market-data `seq=` values therefore have gaps.
- "Snapshot" is a queued read, not a persistence snapshot; persistence snapshots do not exist yet.
- "Rules version" (meaning) and "format version" (layout) are different numbers in the same header.
- "Quarantine" never deletes: it appends a SKIP.

## Common confusions

- **"Passive fill" vs "partial fill".** Passive = the resting side of a trade; partial = not the whole quantity. A passive fill can be complete.
- **"Snapshot" (BookSnapshot) vs "snapshot + fresh journal"** in Spec D46: the latter is a future persistence feature, not built.
- **"Stopped" vs "halted".** STOPPED is a clean stop (documented exit 0; unverified after Ctrl+C, guide 15); HALTED is a failure (exit 2).
- **"Refused" vs "rejected".** Refused = never processed (ERROR line, or `ExchangeException`); rejected = processed by the engine, journalled, `ORDER … status=REJECTED`.

## Questions to test yourself

1. What is the difference between a trade, an execution and a fill in this codebase?
<details><summary>Answer</summary>A <code>Trade</code> is the public record of one match (price, quantity, both order ids, aggressor side). An <code>Execution</code> is the resting order's owner's private copy of its side of that match. <code>book.Fill</code> is what the book returns from <code>fillBest</code>; on the wire, both the taker and the maker receive a <code>FILL</code> line (TAKER and MAKER).</details>

2. Why does a `LEVEL` line's `seq=` jump from 2 to 6 in a session?
<details><summary>Answer</summary><code>seq</code> is the command sequence number. Commands with no public effect (a duplicate reject, a band reject, a failed cancel) consume sequence numbers but produce no market data.</details>

3. Is a POST_ONLY order at a price equal to the best ask a crossing order?
<details><summary>Answer</summary>Yes. <code>crosses</code> is inclusive (<code>restingPrice &lt;= limit</code> for a buy), so it is REJECTED <code>POST_ONLY_WOULD_CROSS</code>.</details>
