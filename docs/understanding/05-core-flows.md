# 05 — Core flows (index)

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** Index of the six end-to-end flows, split across 05a (trading, cancels, market data, resend) and 05b (startup, recovery, compaction, poison pills, shutdown) | **Prerequisites:** 01, 03 | **Related:** 05a, 05b, 06, 07-modules/*
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

The six flows are traced in two files, each kept under ~8,000 words so it can be pasted into an AI on its own:

- **`05a-core-flows-trading.md`**: flows 1–3, what happens to client requests while the exchange is running (login, trading, cancels, cancel-on-disconnect, market data, RESEND).
- **`05b-core-flows-lifecycle.md`**: flows 4–6, how the exchange starts, recovers, compacts its history, survives its own bugs, and stops.

Every flow is described against the code as of 2026-10-27, after the risk fixes recorded as decisions D63–D77 in the Spec. **[Verified]** that code was read for each step, and that the transcripts were captured from a live run that day (Windows) or from a Linux container (exit codes).

## The path every request takes

```
client line --TCP/TLS--> session reader thread: parse, authenticate (LOGIN), stateless checks, session limits
          --queue-->     exchange-writer thread: [CHECK] + stamp -> journal (one fsync per batch)
                                                 -> Ledger.apply (engine + output digest + numbered private events)
                                                 -> OutputListener (private stream to the participant's sessions,
                                                    market data to subscribers)
                                                 -> complete the request's future (refusals, in-flight count)
          --queues-->    session sender threads: write lines to sockets
```

Three facts make this path work, and every flow below relies on them:

1. **One writer thread** owns the sequencer, journal, engine and book; other threads reach them only through the queue (Spec D21, D55). **[Verified]** `src/main/java/exchangeserver/core/ExchangeService.java:286-323`.
2. **Write-ahead:** nothing is processed before its batch is fsynced. **[Verified]** `src/main/java/exchangeserver/core/Exchange.java:223`.
3. **Listener first, then the future (D76):** a request's own lines are offered to the session queues before its callback runs. **[Verified]** `src/main/java/exchangeserver/core/Exchange.java:246-251`.

## The six flows

| # | Flow | File | One-line summary |
|---|---|---|---|
| 1 | A NEW order that trades | 05a | LOGIN (secret, Attach read → `WELCOME oseq=`) → parse → `OrderRequest` → session limits → queue → stamp → fsync → `Ledger.apply` → listener sends the numbered FILL/ORDER lines to every session of each participant, then TRADE/LEVEL → callback reports only refusals |
| 2 | CANCEL and CANCEL_ALL | 05a | Owner-only cancel (someone else's id looks unknown), level update; cancel-on-disconnect submits a journalled CANCEL_ALL when a COD participant's last session closes |
| 3 | SUBSCRIBE, UNSUBSCRIBE, RESEND | 05a | Reads through the queue: the snapshot + subscription (no gap, no duplicate) and the replay of retained private events |
| 4 | Startup, crash recovery, compaction | 05b | Credentials, TLS, bind, accept ("starting"), lock + header check + torn-tail cut, snapshot + two-pass replay with CHECK comparison, sequencer resume, recovery summary; `--compact` writes a snapshot and an empty journal |
| 5 | Poison pills | 05b | Live: SKIP + rebuild from the snapshot + carry on. At startup: `ReplayException`, the right hint, operator `--quarantine` |
| 6 | Shutdown, halt, backpressure, closing a connection | 05b | Signal → hook → close gateway → `stop()` → drain → exit 0 (verified on Linux after SIGINT and SIGTERM); halt → exit 2; BUSY, SESSION_BUSY, RATE_LIMITED; timeouts; close after in-flight requests complete (D76) |

## Key takeaways

- Every request crosses three kinds of thread: a session reader, the single writer, a session sender.
- The reader thread does everything that needs no exchange state: parsing, authentication, field validation, rate and in-flight limits. Nothing it refuses is journalled.
- The writer thread stamps, journals (one fsync per batch), then applies each command to the Ledger and calls the listener before completing the request's future.
- Private lines (FILL, ORDER, CANCELLED, CANCEL_REJECT) carry a per-participant number, `oseq`, and go to every session of that participant; RESEND repeats retained ones.
- Market data (TRADE, LEVEL) goes only to subscribers and carries the command sequence number.
- Recovery = snapshot (if the journal names one) + replay of the journal since it; CHECK records prove the replay matched the live run.
- Every exit path has a defined result: 0 clean stop (signals included), 1 startup or tool failure, 2 halt.

## Common confusions

- **"The reply goes to the session that sent the order."** Since D73 it goes to every session logged in as that participant, numbered in its private stream. Only refusals (BUSY, RATE_LIMITED, …) go to the sending session alone.
- **"The callback sends the ORDER line."** No: the listener does; the callback only reports refusals and keeps the in-flight count (D76).
- **"oseq and seq are the same counter."** `seq` numbers journal records for the whole exchange; `oseq` numbers one participant's private events.

## Questions to test yourself

1. Which three facts does every flow rely on?
<details><summary>Answer</summary>One writer thread owns all exchange state; write-ahead (fsync before processing); the listener runs before the request's future completes (D76).</details>

2. Where does a RATE_LIMITED refusal come from, and is it journalled?
<details><summary>Answer</summary>From the session's reader thread (`ClientSession.admit`), before anything is queued; it is never journalled.</details>

3. What line tells a client the latest number of its private stream when it logs in?
<details><summary>Answer</summary>`WELCOME participant=<p> oseq=<latest>`; it comes from an Attach read on the writer thread, so no event can fall between it and the first live event.</details>
