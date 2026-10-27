# 13 — Glossary

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** every domain term, project-specific name and abbreviation, with a short definition and where it lives in the code | **Prerequisites:** none (use it alongside any guide) | **Related:** 01, 06, 07 (all modules), 09
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

Alphabetical, grouped by first letter. Paths are relative to `src/main/java/exchangeserver/` unless they start with `src/`, `docs/` or `pom.xml`. Every location was read in the current code **[Verified]** unless marked **[Inferred]**.

Abbreviations: **D-n** = design decision n in the Spec; **WAL** = write-ahead log; **L2** = level-2 market data; **IOC/FOK** = immediate-or-cancel / fill-or-kill; **clOrdId** = client order id; **seq** = command sequence number; **oseq** = output sequence number of a participant's private stream; **COD** = cancel-on-disconnect; **STP** = self-trade prevention; **CRC** = cyclic redundancy check; **PBKDF2** = password-based key derivation function 2; **TLS** = transport layer security; **JMH/PIT** = benchmark / mutation tools.

---

## A

- **Acceptor** — the daemon thread `gateway-acceptor` that accepts connections and creates a `ClientSession` each, refusing beyond 256 sessions. It starts *before* replay (sessions answer "starting" until the exchange is attached) and backs off exponentially on `accept()` failures, logging ERROR after 10 in a row (D77). `gateway/TcpGateway.java:164-205`.
- **Adoption (rules adoption)** — a journal with no records yet, written under another `Rules.VERSION`, takes this build's version (its header is rewritten). The way across a rules change, after compaction with the old build (D71). `journal/CommandJournal.java:241-261`, `CommandJournal.adoptedRulesFrom()` at `:216`.
- **Aggressor (taker, incoming order)** — the order being matched now; recorded on every trade as `aggressorSide`. `engine/Trade.java:15`.
- **Append / `appendAll`** — write a batch of sealed records with one write and one fsync; sets the fail-stop flag on error. `journal/CommandJournal.java:143-166`.
- **Archive** — the copy of a journal that compaction keeps, `<journal>.archive-<first>-<last>`; nothing reads it back. `core/Exchange.java:174-196`.
- **Ask** — a sell order resting on the book; the ask side is a `TreeMap` lowest price first. `book/OrderBook.java:42`.
- **Attach (read)** — a queued read that returns a participant's latest oseq; the gateway registers the session for routing in its callback on the writer thread, so `WELCOME … oseq=N` and the first event received leave no gap (D73). `core/Request.java:46-48`, `gateway/ClientSession.java:293-327`.

## B

- **Backpressure / BUSY** — refusing a request when the writer's queue holds `queueCapacity` (100,000) requests. Nothing is journalled. `core/ExchangeService.java:211-240`. Compare **SESSION_BUSY** (per session).
- **Band (price band)** — a priced order must be within ±10% of the last trade price, else REJECTED `PRICE_OUTSIDE_BAND`; no band for priced orders before the first trade. `domain/Rules.java:41-47`, checked at `engine/MatchingEngine.java:188-190`.
- **Base sequence** — the last sequence number of the snapshot a journal continues from, in its header; 0 = none. The first record is base + 1 (D71). `journal/JournalCodec.java:58-60`, `CommandJournal.baseSequence()` at `journal/CommandJournal.java:203`.
- **Batch** — requests the writer takes in one go (≤ `maxBatchSize`, 1,000), stamped, journalled with one fsync, then processed. `core/ExchangeService.java:286-324`, `core/Exchange.java:198-253`.
- **Bid** — a buy order resting on the book; reverse-ordered `TreeMap`. `book/OrderBook.java:41`.
- **BOOK line / `BookSnapshot`** — the snapshot a subscriber gets: `BOOK seq= last= bids= asks=`; `(sequenceNumber, lastTradePrice, bids, asks)`. `gateway/Protocol.java:186-203`, `engine/BookSnapshot.java:14-20`.
- **BOOK_FULL** — reject when the whole book holds `MAX_RESTING_ORDERS` (1,000,000) and the order would rest (D68). `engine/Reason.java:19-20`, `domain/Rules.java:37`.

## C

- **Callback variant** — `ExchangeService.submit/cancel/cancelAll/snapshot/attach/resend(…, BiConsumer)`: runs on the writer thread *after* the command's listener call (D76), or on the caller's thread if refused. A throwing callback is logged (D77). `core/ExchangeService.java:186-240`.
- **Cancel** — removing a resting order by its owner; someone else's order or a gone order yields `UNKNOWN_ORDER` (D48). `book/OrderBook.java:98-108`.
- **CANCEL_ALL / `CancelAllCommand`** — a journalled command that cancels every resting order of one participant, in book priority order; used for cancel-on-disconnect (D72). `command/CancelAllCommand.java:10`, `engine/MatchingEngine.java:136-155`, output `engine/CancelAllOutput.java:11`.
- **Cancel-on-disconnect (COD)** — `LOGIN <p> <secret> COD`; when that participant's last session closes, the gateway submits `CANCEL_ALL`. The cancels land in its private stream (`CANCELLED … reason=CANCEL_ALL`) (D72). `gateway/TcpGateway.java:239-263`.
- **`CancelResult` / `CancelStatus`** — `CANCELLED` (with quantity) or `UNKNOWN_ORDER`. `book/CancelResult.java:6-25`, `book/CancelStatus.java:9-12`.
- **`canRest()`** — true only for `LIMIT` and `POST_ONLY`. `domain/OrderType.java:20-22`.
- **CHECK / `CheckCommand`** — a journal record `CHECK,<seq>,<ts>,<digest>` asserting the output digest of every command before it; written every `checkInterval` (10,000) commands at the start of a batch; replay compares it (D70). `command/CheckCommand.java:16`, written at `core/Exchange.java:200-203`, compared at `core/Replay.java:80-88`.
- **`checkInvariants()`** — package-private test seam re-deriving every book invariant. `book/OrderBook.java:226-267`.
- **Checksum (CRC-32C)** — every journal and snapshot record ends with `,<8 hex digits>`, the CRC-32C of its text; a mismatch is a corrupt record (D69). `journal/Checksums.java:13-17`, `journal/JournalCodec.java:166-187`.
- **clOrdId (client order id)** — the client's id for a new order; unique per participant **within one journal** (after compaction only resting orders keep theirs, D71 revises D49); a repeat is REJECTED `DUPLICATE_CLIENT_ORDER_ID` with the original order id. `domain/Rules.java:29`, map at `engine/MatchingEngine.java:58`.
- **`ClientMessage`** — sealed parsed client lines (Login with secret and COD flag, NewOrder, Cancel, Resend, Subscribe, Unsubscribe, Ping, Quit); `Login.toString` hides the secret. `gateway/ClientMessage.java`.
- **`ClientSession`** — one connection: reader thread, sender thread, bounded outbound queue, login, limits, in-flight count. `gateway/ClientSession.java:53`.
- **Collar (market collar)** — the price range a MARKET order may trade in, fixed at arrival: ±10% around the last trade, or around the opposite **touch** before the first trade (D66); floor/ceil inward. `engine/MatchingEngine.java:274-291`.
- **`Command`** — a stamped, journalled input; sealed: `NewOrderCommand`, `CancelOrderCommand`, `CancelAllCommand`, `SkipCommand`, `CheckCommand` (D12, D31). `command/Command.java`.
- **`CommandInterceptor`** — package-private test seam run just before the engine, live and in replay (where it counts as engine work). `core/CommandInterceptor.java:11-17`.
- **`CommandJournal`** — the write-ahead log: lock, header check, rules adoption, torn-tail cut, sealed append + fsync, streaming read, fail-stop. `journal/CommandJournal.java:46`.
- **Compaction** — `--compact`, run with the exchange stopped: replay, write a snapshot, archive the journal, atomically replace it with an empty one whose base is the snapshot (D71). Bounds recovery; ends clOrdId scope; crosses rules changes. `core/Exchange.java:174-196`.
- **Credentials / users file** — `participant:pbkdf2-sha256:<iterations>:<salt>:<hash>` per line; verified in constant time with a dummy entry for unknown names (D74). `gateway/Credentials.java:34-150`.
- **Crosses** — a buy crosses an ask at or below its limit; a sell a bid at or above it. The single definition (D28): `engine/MatchingEngine.java:293-295`. A crossed book must never exist (`book/OrderBook.java:264`).

## D

- **D-number (D1–D77)** — a numbered design decision with its reason in Spec §3. Next free: D78.
- **Determinism** — engine output depends only on the ordered commands (no clock, counters, randomness, hash-order iteration). `engine/MatchingEngine.java:29-30`.
- **Digest (output digest)** — `OutputDigest`: FNV-1a 64 over a canonical text encoding of every engine output in order; checked by CHECK records (D70). Not cryptographic. `core/OutputDigest.java:25-88`.
- **DIVERGED** — `ReplayException` kind: a CHECK digest differs from what replay produced — this build behaves differently from the one that wrote the journal. `core/ReplayException.java:14-17`.
- **Draining session** — a session whose reader stopped and which waits for its in-flight requests before closing (D76). Test hook `TcpGateway.drainingSessions()`. `gateway/ClientSession.java:111-113`.
- **`drainLevelUpdates()`** — one `LevelUpdate` per level touched since the last call, first-touched order. `book/OrderBook.java:131-145`.
- **`Durable`** — crash-safe file helpers: `writeAtomically` (temp file, fsync, atomic rename, directory fsync), `copyDurably`, `fsyncDirectory` (ignored where a directory cannot be opened, i.e. Windows) (D69, D71). `journal/Durable.java:18-64`.
- **Durable ⇒ visible / visible ⇒ durable** — the second holds; the first does not (a crash after fsync and before the reply). Spec §4.

## E

- **ENGINE_FAILURE** — `ReplayException` kind: the engine threw on a journalled command during replay — a poison pill; `Main` advises `--quarantine <seq>`. `core/ReplayException.java:14-17`, `Main.java:187-198`.
- **`EngineOutput`** — sealed: `OrderOutput`, `CancelOutput`, `CancelAllOutput`. `engine/EngineOutput.java`.
- **`EngineState`** — `(lastSequenceNumber, lastOrderId, lastTradePrice, orders)`: what a snapshot stores for the engine; `MatchingEngine.exportState()`/`restore(...)` (D71). `engine/EngineState.java:14`, `engine/MatchingEngine.java:83-109`.
- **`Exchange`** — the single-threaded live path, poison-pill policy, and the `quarantine`/`compact` tools. `core/Exchange.java:58`.
- **`ExchangeException`** — refusal reasons `BUSY`, `STOPPED`, `HALTED`, `INTERNAL_ERROR`. `core/ExchangeException.java`.
- **`ExchangeService` / `exchange-writer`** — owns the writer thread and the queue. `core/ExchangeService.java:50`, thread created at `:119`.
- **Execution (passive fill)** — a fill on a resting order, addressed to its owner; sent as a `MakerFill` private event, `FILL … liquidity=MAKER`. `engine/Execution.java:11-12`.

## F

- **Fail-stop** — a failed write/fsync refuses every later append; the writer halts; `Main` exits 2. `journal/CommandJournal.java:143-166`.
- **Fill** — (1) `book.Fill`, the book's record of what `fillBest` did; (2) the `FILL` wire line (TAKER or MAKER). Compare **Trade** and **Execution**.
- **`fillBest`** — fill the head of the best level, remove an exhausted order and an emptied level (D44). `book/OrderBook.java:78-95`.
- **Fillability** — the engine's read-only walk of crossing resting orders, stopping at the participant's own order; used by FOK and by the caps (D67, D68). `engine/MatchingEngine.java:299-315`.
- **FOK (fill-or-kill)** — fill completely now or do nothing; CANCELLED with `SELF_TRADE_PREVENTED` if its own order is what blocks it. `engine/MatchingEngine.java:202-208`.
- **Format version** — `JournalCodec.FORMAT_VERSION` = 3 (CRCs, base sequence, new records); v2 journals are refused. `journal/JournalCodec.java:41`. Snapshots have their own (`core/Snapshot.java:44`).
- **fsync / `force(true)`** — force bytes to disk; ~1.5 ms on the dev disk. `journal/CommandJournal.java:143-166`.
- **Future (caller's)** — each future-variant call gets its own `CompletableFuture`, completed on the common pool (`handOff`, D61). `core/ExchangeService.java:170-180`.

## G

- **Gate / Hold** — test helpers that hold the writer: `Gate` (a `CommandInterceptor` in `ExchangeServiceTest`) and `Hold` (an `OutputListener` in `TcpGatewayTest`).
- **Gateway** — TCP/TLS front end: `TcpGateway`, `ClientSession`, `Protocol`, `Credentials`, `Tls`. `gateway/`.
- **Group commit / linger** — wait up to `lingerNanos` for more requests; kept only for benchmark comparison (default off, D52). `core/ExchangeService.java:326-340`.

## H

- **Halt / HALTED** — unrecoverable writer failure; pending requests refused `HALTED`; `Main` exits 2. `core/ExchangeService.java:342-360`.
- **`handOff`** — see Future (caller's).
- **Happens-before** — the Java Memory Model's visibility guarantee (queue put → take, future complete → get, `Thread.start`/`join`). `core/Pending.java:12-13`.
- **Header** — `HEADER,3,<rules>,<base>,<crc>`; checked before anything is changed (D46, D64). `journal/JournalCodec.java:58-85`, `journal/CommandJournal.java:241-261`.
- **`HELD`** — the in-JVM set of held lock files that refuses a second open in the same process before the lock file is touched (D65). `journal/JournalLock.java:27`.

## I

- **Id map** — `orders: orderId → OrderNode`; contains an id iff the order rests. `book/OrderBook.java:43`.
- **Idle timeout** — a logged-in session that sends nothing for 300 s gets `ERROR idle timeout; disconnecting` (socket read timeout, D75). `gateway/ClientSession.java:152-183`.
- **In-flight count / limit (SESSION_BUSY)** — requests a session handed in whose callback has not run; above `maxInFlight` (1,000) new requests get `ERROR <ctx> SESSION_BUSY` (D75); a stopping session closes when it reaches 0 (D76). `gateway/ClientSession.java:279-291`, `:421-443`.
- **Invariant** — a property that must always hold. Spec §2; `book/OrderBook.java:25-28`, `book/PriceLevel.java:20-26`.
- **IOC** — match what crosses now, cancel the remainder. `domain/OrderType.java:6`.

## J

- **JMH** — benchmarks in `src/test/java/exchangeserver/bench/`, `-Pbench`. `pom.xml:93-125`.
- **Journal (WAL)** — the text file of stamped commands, persisted before processing. `journal/JournalCodec.java:16-37`.
- **`JournalCodec`** — encodes/seals and unseals/decodes records; decoding rebuilds requests through `OrderRequest`. `journal/JournalCodec.java:39`.
- **`JournalLock`** — one owner per journal: an exclusive lock on the separate `<journal>.lock` file plus `HELD` (D65). Public so compaction can hold it while replacing the journal. `journal/JournalLock.java:25-84`.

## L

- **L2 market data** — per-price aggregate volume and order count: `LEVEL` lines, `LevelUpdate`. `book/LevelUpdate.java:10`.
- **Last trade price** — band/collar reference; 0 = no trade yet. `engine/MatchingEngine.java:61`.
- **Leaves** — quantity still open after a fill. `book/Fill.java:12`; counted down for taker lines in `gateway/Protocol.java:145-166`.
- **`Ledger`** — what the commands produce, rebuilt together: engine + output digest + output log; replaced as a whole after a poison pill. `core/Ledger.java:22-74`.
- **Level (`PriceLevel`)** — orders at one price on one side, a doubly linked list with sentinels. `book/PriceLevel.java:28`.
- **LIMIT** — match what crosses now, rest the remainder (unless stopped by STP). `domain/OrderType.java:5`.
- **Listener (`OutputListener`)** — `onOutput(output, privateEvents)` on the writer thread, before the request's future completes (D76); the gateway routes private events and market data. Not called in replay. `core/OutputListener.java:19-23`, `gateway/TcpGateway.java:286-312`.
- **Lock file (`<journal>.lock`)** — see `JournalLock`; empty, stays on disk, git-ignored.
- **LOGIN** — `LOGIN <participant> <secret> [COD]`; one attempt per connection; `ERROR login failed` for any wrong part (D74). `gateway/Protocol.java:65-71`, `gateway/ClientSession.java:293-327`.
- **Login timeout** — a connection that does not log in within 10 s gets `ERROR login timeout; disconnecting` (D75).
- **Loopback bind** — the gateway binds 127.0.0.1 unless `--bind` says otherwise (D74). `gateway/TcpGateway.java:68-71`.

## M

- **Maker / passive / resting** — an order on the book; its fills are `MakerFill` events.
- **MARKET** — no price; trades inside the collar; remainder cancelled. `domain/OrderType.java:8`.
- **`MatchingEngine`** — price-time matching, risk checks, STP, outputs; deterministic. `engine/MatchingEngine.java:41`.
- **`MutableClock`** — settable test clock. `src/test/java/exchangeserver/testing/MutableClock.java`.
- **Mutation testing (PIT)** — survivors show untested behaviour. `pom.xml:126-169`; results in Spec §7.

## O

- **Open-order cap** — at most 10,000 resting orders per participant; checked only for an order that would rest (D68). `engine/MatchingEngine.java:193-200`.
- **`Order` / order id** — an accepted order (request + id + timestamp); order ids are issued by the sequencer, strictly increasing. `domain/Order.java:16`, `command/Sequencer.java:48-52`.
- **`OrderRequest`** — the only field-validation layer (D47). `domain/OrderRequest.java:15`.
- **`OrderStatus`** — `FILLED`, `RESTED`, `CANCELLED`, `REJECTED`. `engine/OrderStatus.java`.
- **oseq (output sequence number)** — the number of an event in a participant's private stream: 1, 2, 3, … per participant, derived from the commands so replay rebuilds it and snapshots keep the last one (D73). Printed as `oseq=` on FILL/ORDER/CANCELLED/CANCEL_REJECT and in `WELCOME`. `core/OutputLog.java:31-123`.
- **`OutputLog`** — numbers private events per participant and keeps the last `outputRetention` (1,000) for RESEND. `core/OutputLog.java:31`.

## P

- **Participant** — the authenticated identity owning orders; pattern `[A-Za-z0-9_]{1,16}`; must be in the users file (D74). `domain/Rules.java:28`.
- **PBKDF2** — PBKDF2-HMAC-SHA256 with a random 16-byte salt and 210,000 iterations by default: makes a stolen users file expensive to brute-force. `gateway/Credentials.java:36`, `:120-136`.
- **`Pending`** — a request in flight with its own future, its command, and the writer-owned `refused` flag. `core/Pending.java:16-21`.
- **Poison pill** — a journalled command that makes the engine throw; quarantined live, operator-quarantined at startup (D51).
- **POST_ONLY** — rests without matching; REJECTED if it would cross (equal price counts), or at a cap. `engine/MatchingEngine.java:209-213`.
- **Price-time priority** — best price first, then earliest arrival. `book/PriceLevel.java:64-88`.
- **Private event / private stream** — `PrivateEvent` (`OrderReply`, `CancelReply`, `MakerFill`): the numbered messages only one participant may see, sent to *every* session of that participant in oseq order (D73; replies used to go only to the requesting session). `core/PrivateEvent.java:12-32`.
- **`Protocol`** — parsing and line formatting; its header comment is the protocol documentation. `gateway/Protocol.java:16-45`.

## Q

- **Quarantine** — appending `SKIP <seq>` so replay passes over a poison pill. Live: `core/Exchange.java:278-289`; operator tool `--quarantine <seq>` (refuses a command already quarantined, D77): `core/Exchange.java:141-171`.
- **Queue** — the single `LinkedBlockingQueue<Pending<?>>` into the writer; capacity enforced under a lock (D56). `core/ExchangeService.java:101`.

## R

- **Rate limit (RATE_LIMITED)** — a token bucket per session: 1,000 requests/s, bursts of 2,000; beyond it `ERROR <ctx> RATE_LIMITED`, nothing queued (D75). `gateway/ClientSession.java:279-291`.
- **`Reason`** — `DUPLICATE_CLIENT_ORDER_ID`, `PRICE_OUTSIDE_BAND`, `MAX_OPEN_ORDERS`, `BOOK_FULL`, `POST_ONLY_WOULD_CROSS` (rejects, `isReject()`), and `SELF_TRADE_PREVENTED` (with CANCELLED). Replaced `RejectReason`. `engine/Reason.java:12-28`.
- **Recovery summary** — the line `Main` prints at start: `Recovered through sequence N [from snapshot B]: replayed R commands, cut T torn bytes[, quarantined …][, journal moved from rules V]` (D77). `Main.java:154-157`, `Exchange.Recovery` at `core/Exchange.java:78-80`.
- **Refused vs rejected** — refused: never processed (ERROR line or `ExchangeException`); rejected: processed and journalled, `status=REJECTED`.
- **Replay** — rebuild the ledger from the snapshot the header names plus the journal: verify, then feed and compare CHECKs (D60, D70, D71). `core/Replay.java:30-131`.
- **`ReplayException`** — replay failure at a known record: `ENGINE_FAILURE` or `DIVERGED`, with the sequence number, so `Main` gives fitting advice (D77). `core/ReplayException.java:12-37`.
- **`Request`** — sealed: `Submit`, `Cancel`, `CancelAll` (journalled) and `Snapshot`, `Attach`, `Resend` (reads). `core/Request.java:20-57`.
- **RESEND / RESENT / RESEND_GAP** — `RESEND <n>` asks for the retained private events from oseq n; they come back with ` resend=1`, then `RESENT last=<latest>`; if n is older than what is retained, `ERROR RESEND_GAP first=<oldest retained>` comes first (D73). `gateway/ClientSession.java:342-371`, `core/ResendResult.java:7`.
- **`Response`** — what a new order's submitter is told; invariants in its constructor (D22). `engine/Response.java:15-56`.
- **Retention** — `Exchange.Options.outputRetention` (1,000 per participant, `--resend-window`). `core/Exchange.java:64-75`.
- **Rules version** — `Rules.VERSION` = 2; in the header; records refuse other versions, an empty journal adopts. `domain/Rules.java:21`.

## S

- **Self-trade prevention (STP)** — the match loop stops when the next resting order to trade with is the incoming order's own participant's; the remainder is CANCELLED with `SELF_TRADE_PREVENTED`, whatever the type; a LIMIT does not rest (D67). `engine/MatchingEngine.java:224-227`, `:240-252`.
- **Sentinel** — dummy head/tail nodes of a level's list. `book/PriceLevel.java:31-32`.
- **Sequence number (seq)** — contiguous number of every journal record (NEW, CANCEL, CANCEL_ALL, SKIP, CHECK), from base + 1. Market-data `seq=` therefore has gaps. `command/Sequencer.java:13-14`.
- **`Sequencer`** — the only clock reader and counter. `command/Sequencer.java:20-94`.
- **SESSION_BUSY** — see In-flight limit.
- **Settings** — `ExchangeService.Settings` (`queueCapacity`, `maxBatchSize`, `lingerNanos`, `clock`, `Exchange.Options`) and `TcpGateway.Settings` (bind address, port, credentials, TLS, sessions, outbound, timeouts, in-flight, rate, burst). `core/ExchangeService.java:59-92`, `gateway/TcpGateway.java:52-98`.
- **Single writer** — exactly one thread touches sequencer, journal, ledger and book.
- **SKIP / `SkipCommand`** — `SKIP,<seq>,<ts>,<skippedSeq>`. `command/SkipCommand.java:14-25`.
- **Slow consumer** — a session whose 10,000-line outbound queue is full; disconnected (D54). `gateway/ClientSession.java:116-124`.
- **Smart batching** — take what is queued, one fsync; the default (D52).
- **Snapshot (two meanings)** — (1) the `Snapshot` *read* returning a `BookSnapshot` for SUBSCRIBE (≤ 100 levels/side, D62); (2) the persistent **snapshot file** `<journal>.snapshot-<seq>` written by compaction: last sequence, last order id, last timestamp, last trade price, digest, resting orders in book priority, each participant's last oseq, an `END` line, a CRC per line (D71). `core/Snapshot.java:21-137`.
- **States** — `RUNNING`, `STOPPING`, `STOPPED`, `HALTED`. `core/ExchangeService.java:52-57`.
- **Stop marker** — the private `Pending` `stopMarker`, enqueued by `stop()` under the lock. `core/ExchangeService.java:96`, `:245-258`.
- **Subscription** — a session receiving TRADE/LEVEL lines; guarded against SUBSCRIBE/UNSUBSCRIBE races. `gateway/ClientSession.java:373-411`.

## T

- **Taker** — see Aggressor; `FILL … liquidity=TAKER`.
- **TLS** — optional transport encryption (TLS 1.3/1.2) with a PKCS#12 keystore; password from `ES2_TLS_PASSWORD` (D74). `gateway/Tls.java:20-67`.
- **Token bucket** — the rate-limit algorithm: tokens refill continuously up to `burst`; each request takes one; none left means refused. `gateway/ClientSession.java:279-291`.
- **Torn tail** — bytes after the last `\n`; cut on open after the header check. `journal/CommandJournal.java:263-297`.
- **Touch** — the best bid and best ask; since D66 also the collar reference before the first trade.
- **Trade** — the public record of one match; price = the resting level's; timestamp = the incoming command's (D34). `engine/Trade.java:15-27`.

## U

- **`UNKNOWN_ORDER`** — cancel outcome for a missing, gone or not-owned order. `book/CancelStatus.java:3-12`.
- **Unlink** — O(1) removal of a node from its level. `book/PriceLevel.java:124-142`.

## W

- **WELCOME** — `WELCOME participant=<p> oseq=<latest>`: sent after the Attach read registered the session. `gateway/Protocol.java:140-142`.
- **`withinBand`** — exact integer band check. `domain/Rules.java:44-47`.
- **Write-ahead** — fsync a command before processing it. `core/Exchange.java:223`.

---

## Key takeaways

- Three "fill" words: **Trade** (public), **Execution** (maker's), **Fill** (book record / wire line).
- Two sequence numbers: **seq** (every journal record, global) and **oseq** (per participant, private stream).
- Two "snapshot"s: the queued read for SUBSCRIBE, and the persistent snapshot file from compaction.
- Three replay guards: rules version, format version, CHECK digests.
- Refusals come in levels: session (`RATE_LIMITED`, `SESSION_BUSY`), exchange (`BUSY`, `STOPPED`, `HALTED`), engine rejects (journalled `REJECTED`).
- "Cancelled" now has three sources: a client cancel, IOC/MARKET/FOK leftovers (sometimes `SELF_TRADE_PREVENTED`), and CANCEL_ALL (`reason=CANCEL_ALL`).

## Common confusions

- **"Passive fill" vs "partial fill"** — passive = the resting side; partial = not the whole quantity.
- **"Reply goes to the requesting session"** — no longer: replies are private events, sent to every session of the participant (D73). Refusals still go only to the requester.
- **"D63 closes after a barrier read"** — replaced by D76's in-flight count.
- **"Stopped" vs "halted"** — STOPPED exits 0 (verified on Linux for SIGINT and SIGTERM); HALTED exits 2.
- **"SELF_TRADE_PREVENTED is a reject"** — no: it goes with CANCELLED, and the order may have traded with others first.
- **"CHECK is a command the engine runs"** — no: replay compares it; the engine never sees it.

## Questions to test yourself

1. What is the difference between `seq` and `oseq`?
<details><summary>Answer</summary><code>seq</code> numbers every journal record exchange-wide (market data carries it, with gaps). <code>oseq</code> numbers one participant's private events 1, 2, 3, …, without gaps, rebuilt by replay; RESEND works on it.</details>

2. A client gets `ERROR clOrdId=c9 RATE_LIMITED`. Was anything journalled?
<details><summary>Answer</summary>No. Session limits are checked on the reader thread before the request reaches the queue (D75).</details>

3. Is a POST_ONLY order at the best ask's price crossing?
<details><summary>Answer</summary>Yes: <code>crosses</code> is inclusive, so it is REJECTED <code>POST_ONLY_WOULD_CROSS</code>.</details>

4. What does `ERROR RESEND_GAP first=3` tell the client?
<details><summary>Answer</summary>Events before oseq 3 are no longer retained (or are older than the last compaction); the lines that follow start at 3.</details>
