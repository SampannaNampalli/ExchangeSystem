# 09 — Design decisions (index)

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** how to read the decision records, and an index of all 38 records across part 1 (09a: foundations, D1–D65) and part 2 (09b: risk fixes, D66–D77) | **Prerequisites:** 01, 05 | **Related:** 09a, 09b, 07-modules/*, 08, 11, 15
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

This codebase comes with its decisions written down: `docs/ExchangeServer2_Spec.md` §3 numbers them **D1–D77**, each with a reason, and code comments cite them, e.g. "(D44)" [Verified]. The records in guides 09a and 09b explain the significant ones in a fixed shape: **Decision · Why · Alternatives not taken · Trade-offs and consequences**. Labels:
- **[Verified — Spec Dnn]**: the Spec states the reason.
- **[Verified — path:line]**: the code or a comment states it.
- **[Inferred]**: a reconstruction (alternatives and trade-offs mostly are).
- **⚠ Gap**: a weak spot in the stated reasoning.

Where the decisions came from:

| Range | Made | Guide |
|---|---|---|
| D1–D43 | The original project's Spec v8 (the user's earlier `Exchange Server` project) | 09a |
| D44–D62 | The rewrite (ExchangeServer2) | 09a |
| D63–D65 | 2026-10-27, problems found while writing these guides (lost replies on QUIT, journal open damaging foreign files, no single-writer lock) | 09a |
| D66–D77 | 2026-10-27, when the user asked for every risk in guide 11 to be fixed, including the security findings first deferred | 09b |

The guide was split in two because one file would have passed the 8,000-word limit.

## Index of records

| # | Decision | Spec | Guide | Revised by |
|---|---|---|---|---|
| 1 | One writer thread owns all exchange state | D21, D55 | 09a | — |
| 2 | Journal the inputs, not the state | D12, D41 | 09a | D71 (snapshots as checkpoints) |
| 3 | Plain-text journal | D36 | 09a | D69 (CRC, v3) |
| 4 | Write-ahead, one fsync per batch, smart batching | D37 → D52 | 09a | — |
| 5 | Replay through the same engine; determinism rules | D12, D34, D40, D41 | 09a | — |
| 6 | The Sequencer is the only clock reader and counter | D32, D33, D55 | 09a | — |
| 7 | Packages enforce book ownership | D1, D3, D44 | 09a | — |
| 8 | TreeMap of levels + linked list per level + id map; no cached best pointer | D5–D9, D59 | 09a | — |
| 9 | Eager removal of empty levels | D10, D14, D18 | 09a | — |
| 10 | Validation layers; state-dependent rejects journalled | D13, D19, D47 | 09a | — |
| 11 | Client order id unique per participant, forever | D45, D49 | 09a | D71 (per journal) |
| 12 | Price band and market collar fixed at arrival | D50 | 09a | D66 (collar before the first trade) |
| 13 | Poison pills: quarantine live, stop at startup | D51 | 09a | D71, D77 |
| 14 | Rules version in the journal header | D20, D46 | 09a | D70, D71 |
| 15 | Two-pass streaming replay | D42, D60 | 09a | D71 (from base+1) |
| 16 | Reads go through the queue | D57 | 09a | D73 (Attach, Resend reads) |
| 17 | Bounded session queues; slow consumers disconnected | D54 | 09a | D73 (RESEND) |
| 18 | Unbounded queue, capacity checked under the hand-in lock | D56 | 09a | D75 (per-session limits) |
| 19 | Callers get their own future | D61 | 09a | D76 (listener before future) |
| 20 | Bounded SUBSCRIBE snapshot | D62 | 09a | — |
| 21 | Fail-stop; only the writer closes the journal | D39, D58 | 09a | — |
| 22 | Text line protocol, thread-per-connection | D53 | 09a | D74, D75 |
| 23 | Close a session only after its requests are answered | D63 | 09a | D76 (in-flight count) |
| 24 | Check the journal header before changing the file | D64 | 09a | D71 (empty journal adopts new rules) |
| 25 | One process per journal: a lock file | D65 | 09a | D71 (`JournalLock` class) |
| 26 | Build and dependencies | Vision §8 | 09a | — |
| 27 | Market collar before the first trade | D66 | 09b | |
| 28 | Self-trade prevention: cancel the incoming remainder | D67 | 09b | |
| 29 | Caps only for orders that would rest; global `BOOK_FULL` | D68 | 09b | |
| 30 | Journal format v3: CRC, directory fsync, CRLF message | D69 | 09b | |
| 31 | Output digest checked by CHECK records | D70 | 09b | |
| 32 | Snapshots and offline compaction | D71 | 09b | |
| 33 | CANCEL_ALL and opt-in cancel-on-disconnect | D72 | 09b | |
| 34 | Numbered private stream per participant, with RESEND | D73 | 09b | |
| 35 | Authentication and transport | D74 | 09b | |
| 36 | Per-session limits | D75 | 09b | |
| 37 | Listener first, then the future; close when nothing is in flight | D76 | 09b | |
| 38 | Operations: say what happened, advise what fits | D77 | 09b | |

## Key takeaways

- Look up the D-number in Spec §3 before guessing a reason: most decisions state theirs.
- The core triad is unchanged: **single writer**, **write-ahead input journal**, **deterministic replay through the same engine**. Every later decision protects or extends one of these.
- Two decisions were settled by measurement: smart batching (D52) and no cached best pointer (D59).
- The risk fixes revised five earlier decisions in substance: D49 (ids per journal), D50 (collar before the first trade), D63 (in-flight close), D46 (crossing rules changes), and the non-goal "authentication; TLS".
- Four risk-fix choices were the user's: the authentication model, the self-trade policy, missed-fill recovery, and delivery.

## Common confusions

- *"09a is out of date."* Its records describe the foundations as decided; each one that changed carries a **Revised by** line pointing to 09b.
- *"Every D-number has a record here."* Only the significant ones; the Spec has all 77 in short form.
- *"Revised means the old reason was wrong."* Usually it means the cost the old record listed (growth, no checksum, no recovery) has now been paid down.

## Questions to test yourself

1. Which decisions protect the "deterministic replay" leg of the triad, across both parts?
<details><summary>Answer</summary>Records 5 and 6 (determinism rules, the Sequencer as the only clock/counter), 14 (rules version), 31 (CHECK digests detect divergence) and 32 (snapshots load state instead of re-running old rules). Also 28/29/33 live in the engine because they are journalled behaviour.</details>

2. Which record in part 1 was replaced the same day it was made, and why?
<details><summary>Answer</summary>Record 23 (D63): its barrier read was refused once the exchange was stopping, so a session closing during shutdown lost replies; D76 replaced it with an in-flight count (record 37). Record 25 (D65) also changed implementation the same day (the lock moved to a separate file for POSIX reasons).</details>
