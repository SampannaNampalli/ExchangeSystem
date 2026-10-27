# 05 — Core flows (index)

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** Index of the six end-to-end flows, split across 05a (trading and market data) and 05b (startup, recovery, poison pills, shutdown) | **Prerequisites:** 01, 03 | **Related:** 05a, 05b, 06, 07-modules/*
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

The six flows are traced in two files, each kept under ~8,000 words so it can be pasted into an AI on its own:

- **`05a-core-flows-trading.md`** — flows 1–3: what happens to client requests while the exchange is running.
- **`05b-core-flows-lifecycle.md`** — flows 4–6: how the exchange starts, recovers, survives its own bugs, and stops.

Every flow is traced from its trigger to its final effect through each method and file it touches, with a Mermaid
sequence diagram and the data shape at each step. All of them pass through the same backbone **[Verified]**
(`src/main/java/exchangeserver/core/Exchange.java:116-166`):

```
session reader: line -> ClientMessage -> OrderRequest (stateless checks)
      -> ExchangeService queue (one lock: state + capacity)
exchange-writer, per batch: stamp all (Sequencer) -> journal.appendAll (one write, one fsync)
      -> for each: engine -> complete the future (reply) -> OutputListener (passive fills, market data)
session sender(s): bounded outbound queue -> socket
```

## The six flows

| # | Flow | File | Summary |
|---|---|---|---|
| 1 | NEW order that trades, over TCP | 05a | Bytes → `readLine` → `Protocol.parse` → `OrderRequest.of` → `ExchangeService.submit` (callback attached before queuing) → writer batch → `Sequencer.stampNewOrder` → `CommandJournal.appendAll` (fsync) → `MatchingEngine.onNewOrder` (checks, matching loop over `OrderBook.fillBest`, outcome switch) → reply callback (FILL TAKER, ORDER) → `TcpGateway.onOutput` (FILL MAKER, TRADE, LEVEL) → sender threads. Explains why the reply precedes the market data. |
| 2 | CANCEL | 05a | Participant comes from the session; stamped with a sequence number but no order id; journalled even when it fails; `OrderBook.cancel` answers `UNKNOWN_ORDER` for missing *or* someone else's orders; `removeNode` → `PriceLevel.unlink` in O(1) and removes an emptied level, reported as `LEVEL … qty=0`. |
| 3 | SUBSCRIBE / UNSUBSCRIBE | 05a | A depth-100 snapshot is a read through the queue (never journalled); its callback sends SUBSCRIBED + BOOK and joins the subscriber set in the same writer step, so there is no gap or duplicate; `subscriptionLock` plus numbered subscriptions handle UNSUBSCRIBE races. |
| 4 | Startup and crash recovery | 05b | `Main` binds the port → `Exchange.open` → `CommandJournal.open` (lock, header check, torn-tail cut, header write) → `Replay.run` (verify pass, feed pass) → `Sequencer.resume` → writer and acceptor start. Includes the crash-recovery run seen in this session. |
| 5 | Poison pills | 05b | Live: SKIP record, rebuild via `Replay.feed`, batch continues, caller gets INTERNAL_ERROR. At startup: startup fails (exit 1); the operator runs `--quarantine <seq>`, which is refused while a server holds the journal lock. |
| 6 | Shutdown, halt, backpressure, closing a connection | 05b | Ctrl+C hook → gateway close → `stop()` (STOP marker under the enqueue lock) → drain → writer closes journal → exit (documented 0; unverified after Ctrl+C); halt → everyone refused HALTED → exit 2; BUSY on a full queue; slow readers disconnected; QUIT / end of stream close only after the session's replies (D63). |

## Key takeaways

- One backbone serves every flow: reader (parse, validate) → queue → writer (stamp, journal, engine, reply,
  listener) → senders.
- Write-ahead is per batch: nothing in a batch is processed before its single fsync.
- Replies are delivered before the market data the same command caused, per session.
- Only the journal persists; startup rebuilds all state by replaying it through the same deterministic engine.
- Failures split three ways: bad input → answered at the gateway, never journalled; engine bug → quarantined live;
  journal failure or `Error` → halt, exit 2.

## Common confusions

- **"05 is one long file."** It is an index; the traces are in 05a and 05b.
- **"Each flow has its own code path."** Startup replay, the live path and the poison-pill rebuild all run the
  same `MatchingEngine`; only who feeds it differs.

## Questions to test yourself

1. Which flows journal something, and what?
<details><summary>Answer</summary>Flow 1 (a NEW record, even for state-dependent rejects), flow 2 (a CANCEL record, even for UNKNOWN_ORDER), flow 4 (a HEADER if the file is new; a torn tail may be cut), flow 5 (a SKIP record, live or by the operator tool). Flow 3 (reads) and flow 6 (shutdown, refusals, close-after-replies reads) journal nothing.</details>

2. Which thread runs the reply callback for a NEW that was accepted, and which for one refused BUSY?
<details><summary>Answer</summary>Accepted: the `exchange-writer` thread, when it completes the future inside `Exchange.process`. Refused: the session's reader thread, inside `ExchangeService.enqueue`, after the lock is released.</details>
