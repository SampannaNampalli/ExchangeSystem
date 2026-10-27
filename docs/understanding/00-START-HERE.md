# 00 — Start Here

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** what the project is, who these guides are for, reading order and study time, how to study them with an AI, the labelling convention, which code state they describe | **Prerequisites:** none | **Related:** 01, 13, 14, 15
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

ExchangeServer2 is a small but complete **trading exchange for one instrument**, written in plain Java 17 with no
runtime dependencies. Clients connect over TCP (optionally TLS), log in with a secret, and type text lines such as
`NEW a1 SELL LIMIT 10 100` ("sell 10 at 100, my id for this order is a1"). The exchange keeps an **order book**
(all buy and sell orders that are waiting), matches incoming orders against it by **price-time priority** (best
price first; at the same price, first come first served), and tells everyone involved what happened.

Behind that simple surface sit the design problems real exchanges have to solve, which is the point of the
project (Vision §1, **[Verified]** `docs/ExchangeServer2_Vision.md`):

- **Durability:** every accepted command is written to a **journal** (an append-only text file, every record with
  a checksum) and forced to disk with `fsync` *before* it has any effect. This is **write-ahead logging**.
- **Recovery:** after a crash, the state is rebuilt by **replaying** the journal through the same matching code,
  starting from the latest **snapshot** if the journal was compacted; **CHECK** records prove the replay produced
  the same outputs as the live run.
- **Determinism:** for replay to rebuild the identical book, the matching engine must depend only on its input
  commands: no clock, no random numbers, no counters of its own.
- **Concurrency:** exactly one thread (the **writer thread**, named `exchange-writer`) touches the book; every
  other thread hands requests to it through one queue.
- **Networking and security:** a thread-per-connection gateway with authentication, timeouts, per-session limits,
  a numbered private message stream per participant that a reconnecting client can ask to have resent, and no way
  for a slow client to stall the exchange.

The code was written entirely by an AI coding agent on 2026-10-27 at the owner's request, as a rewrite of the
owner's earlier hand-written project, and then extended the same day to fix every risk found while these guides
were being written (**[Verified]** Vision §2–§3, Spec §10–§11, `CLAUDE.md`). The owner is the intended reader of
these guides.

## Who these guides are for

You are a capable developer who did not write this code and wants to understand it the way its author would:

1. **Explain** how it works and why it is shaped this way.
2. **Predict** what it does for a given input (a line typed by a client, a crash at a given moment).
3. **Change** it with confidence (which files to touch, which invariants to keep, which tests to add).
4. **Debug** it (which thread does what, where to put a breakpoint, what a log line means).

You do not need prior knowledge of exchanges; guide 13 (glossary) defines every term, and each guide defines terms
on first use.

## Reading order and study time

Times are for careful reading with the repo open, including the self-test questions. Halve them for a skim.

| # | Guide | What you get | Time |
|---|---|---|---|
| 00 | START-HERE (this) | Orientation | 10 min |
| 01 | big-picture | The problem, features, architecture diagram, the 10 things to know first | 45 min |
| 02 | tech-stack | Java 17 features, concurrency, NIO, security (PBKDF2, TLS), Maven/JUnit/JMH/PIT/Docker, as used here | 75 min |
| 03 | codebase-map | Every file and what it is for; conventions | 35 min |
| 04 | running-and-tooling | Users, build, run, test, debug, operator tools, with real output | 50 min (+40 min doing it) |
| 05 | core-flows | Index of the flows; read 05a and 05b | 5 min |
| 05a | core-flows-trading | Order, fill, private stream, cancel, market data, RESEND, with sequence diagrams | 90 min |
| 05b | core-flows-lifecycle | Startup and recovery, compaction, poison pills, shutdown and halt | 75 min |
| 06 | data-and-state | Data models, journal and snapshot formats, wire protocol, where state lives | 60 min |
| 07 | modules/domain-and-commands | `Rules`, `OrderRequest`, `Order`, `Sequencer`, commands | 45 min |
| 07 | modules/book | `OrderBook`, `PriceLevel`, linked lists with sentinels, invariants | 60 min |
| 07 | modules/engine | `MatchingEngine`: order types, risk checks, self-trade prevention, outputs | 75 min |
| 07 | modules/journal | `CommandJournal`, `JournalCodec`, `JournalLock`: fsync, checksums, torn tail, lock | 60 min |
| 07 | modules/exchange-and-replay | `Exchange`, `Replay`: the live path, poison pills, the compaction tool | 60 min |
| 07 | modules/ledger-and-snapshots | `Ledger`: output digest and CHECK records, private streams (oseq, RESEND), snapshots | 60 min |
| 07 | modules/exchange-service | `ExchangeService`: writer thread, queue, batching, shutdown | 75 min |
| 07 | modules/gateway | `TcpGateway`, `ClientSession`, `Protocol`, `Credentials`, `Tls` | 90 min |
| 07 | modules/client-and-main | `ExchangeClient`, `ClientMain`, `Main` | 30 min |
| 08 | cross-cutting | Errors, logging, security, config, concurrency, performance | 45 min |
| 09 | design-decisions | How to read the decision records; index | 5 min |
| 09a | design-decisions-foundations | Decision records D1–D65 | 45 min |
| 09b | design-decisions-risk-fixes | Decision records D66–D77 | 30 min |
| 10 | testing | Strategy, seams, coverage, how to write a test | 45 min |
| 11 | risks-and-smells | Ranked honest review | 40 min |
| 12 | change-recipes | Step-by-step "how do I…" | 40 min |
| 13 | glossary | Every term, linked to code | reference |
| 14 | exercises | Self-checks, predict-then-verify, tracing, hands-on, break-it | 4–8 h |
| 15 | open-questions | What could not be determined | 15 min |

Total: roughly 22–28 hours including the exercises. A good three-day plan: day 1 = 00–05; day 2 = 06–07 (all
modules); day 3 = 08–15 with the exercises.

**Minimum path** if you only have two hours: 01 → 05 (the new-order and restart flows) → 11.

## How to use these guides with an AI assistant

The guides are written so that an AI that **cannot see the repository** can still tutor you:

1. Paste `AI-TUTOR-PROMPT.md` first. It tells the AI to act as a tutor for this project, to treat the guides as
   the source of truth, to say when a question goes beyond them, and to separate **[Verified]** facts,
   **[Inferred]** claims and its own general knowledge.
2. Then attach either one guide (best for focused study) or `FULL-CONTEXT.md` (all guides joined in reading order
   with a table of contents, for AIs that accept one large attachment).
3. `FULL-CONTEXT.md` is generated. After editing any guide, regenerate it from the repository root:

   ```
   java docs/understanding/build-full-context.java
   ```

   (Java 11+ runs a single `.java` file directly; the JDK 17 you already need for the project is enough.)

4. When the AI and a guide disagree with the real code, the code wins. Every guide cites `path:LINE` so you can
   check in seconds.

## The labelling convention

Every non-trivial claim carries one of two labels:

- **[Verified]**: read in the code or the design docs, or seen when the program or the tests ran. It comes with a
  citation: a repo-relative `path:LINE` (for example `src/main/java/exchangeserver/core/Exchange.java:198`), or a
  design-doc reference such as "Spec D52" (decision 52 in `docs/ExchangeServer2_Spec.md`) or "Spec §5".
  "[Verified — observed]" means seen running.
- **[Inferred]**: reasoning about intent or behaviour that the code or docs do not state outright. Treat it as a
  well-founded hypothesis; several exercises in guide 14 ask you to confirm or refute one.

Anything that could not be determined is listed in guide 15 (open questions) rather than guessed.

A note on "why": this project is unusual in that most design reasons are **written down** in the Spec as numbered
decisions (D1–D77). Where a guide gives a reason with a D-number, the reason itself is **[Verified]** (it is what
the author recorded); whether the reason is *good* is a separate matter discussed in guides 09 and 11.

## What state of the code these guides describe

These guides describe the code on branch `fix/risks` as of 2026-10-27 (**[Verified]** git and Spec §10–§11):

1. **The rewrite** (initial commit `b3091a8`): roadmap steps 1–7 (order book, order types, journal and replay,
   single writer thread, TCP gateway, risk checks and market data, benchmarks, poison-pill recovery).
2. **Fixes found while surveying the code** (Spec §10, D63–D65): a flaky test, replies lost on `QUIT`, the journal
   opener cutting the last line of a file that was not a journal, and no lock against a second process.
3. **"Fix all the risks"** (Spec §11, D66–D77), which closed every item of the original guide 11:
   - **Engine** (`Rules.VERSION` 2): self-trade prevention (an incoming order never trades with its own
     participant's resting order; its remainder is cancelled, D67), resting caps only for orders that would rest
     plus a global `BOOK_FULL` cap (D68), a market-order collar before the first trade anchored on the best opposite
     price (D66), and `CANCEL_ALL` (D72).
   - **Journal format v3**: a CRC-32C on every record, directory fsync, a clear error for CRLF-converted files
     (D69); `CHECK` records holding a digest of every output so far, compared on replay (D70).
   - **Snapshots and compaction**: `--compact` writes the state to a snapshot and starts an empty journal that
     continues from it (D71).
   - **Private stream**: replies and fills are numbered per participant (`oseq=`), sent to every session of that
     participant, and can be asked for again with `RESEND` (D73); opt-in cancel-on-disconnect (`COD`, D72).
   - **Security and limits**: the gateway listens on 127.0.0.1 by default, `LOGIN` needs a secret checked against
     salted PBKDF2 hashes, TLS is optional (D74); login and idle timeouts, a per-session in-flight cap and a rate
     limit (D75).
   - **Operations**: a session closes only when nothing it sent is still in flight, even during shutdown (D76);
     startup prints what recovery did, failure hints fit the failure, and a stop by signal exits 0 (D77).
- Test status: **453 tests, all green on Windows**; the suite also passed on Linux (446 tests at the time; Linux run in Docker, `eclipse-temurin:17`)
  (**[Verified — observed]**).
- If you are reading this after further commits, check `git log` and the Spec's decision table: anything past D77
  is newer than these guides.

## Key takeaways

- ExchangeServer2 is a one-instrument exchange: authenticated gateway → one queue → one writer thread → journal
  (fsync) → deterministic matching engine → order book, with numbered private replies and public market data out.
- Its purpose is learning: every design choice is meant to be justified, and the justifications are recorded as
  D-numbers in `docs/ExchangeServer2_Spec.md`.
- Read 01 → 05 first; the module guides (07) go deep once you have the flows.
- **[Verified]** = cited from code/docs or observed running; **[Inferred]** = reasoned. Unknowns live in guide 15.
- The guides describe branch `fix/risks` with decisions up to D77; 453 tests pass on Windows; the suite also passed on Linux.
- With an AI: paste `AI-TUTOR-PROMPT.md`, then a guide or `FULL-CONTEXT.md`; regenerate the latter with
  `java docs/understanding/build-full-context.java`.

## Common confusions

- **"The exchange" means three different classes.** `Exchange` is the single-threaded core (stamp, journal,
  process); `ExchangeService` wraps it with the writer thread and queue; `Main` starts the whole process. Guide 03
  has the full map.
- **"Order id" vs "client order id".** The order id is a number the exchange assigns (1, 2, 3, …); the client order
  id (`clOrdId`, e.g. `a1`) is chosen by the client and must be unique per participant within one journal (until
  the next compaction; a resting order keeps its id across it).
- **"Sequence number" vs "order id" vs "oseq".** Every journalled record (new order, cancel, cancel-all, skip,
  check) gets the next sequence number; only new orders get an order id; `oseq` numbers one participant's private
  messages (1, 2, 3, … per participant).
- **The Vision and the Spec are different documents.** Vision = purpose, principles, roadmap. Spec = exact code
  state and decisions. These guides explain both but neither replaces them.

## Questions to test yourself

1. Name the five design problems this project exists to practise.
   <details><summary>Answer</summary>Durability (write-ahead journal with fsync and checksums), recovery (replay,
   from a snapshot when compacted, verified by CHECK records), determinism (engine depends only on its inputs),
   concurrency (single writer thread fed by a queue), networking and security (an authenticated gateway with
   limits that a slow client cannot stall).</details>
2. A guide says "**[Verified]** Spec D52". What does that tell you, and what does it not tell you?
   <details><summary>Answer</summary>That the stated reason is what the author recorded as decision 52 in
   `docs/ExchangeServer2_Spec.md`. It does not tell you the reason is correct or still current; guides 09 and 11
   judge that separately.</details>
3. What is the difference between an order id, a sequence number and an oseq?
   <details><summary>Answer</summary>Order id: assigned by the exchange to each new order. Sequence number: assigned
   to every journalled record, in order. Oseq: the number of a message in one participant's private stream (its
   replies, fills and cancels), used for `RESEND`.</details>
4. Which three groups of changes do the guides describe on top of the initial commit?
   <details><summary>Answer</summary>The survey fixes (D63–D65 plus a flaky test), and the "fix all the risks"
   work (D66–D77: engine policy, journal v3 with checksums and CHECK digests, snapshots and compaction, the numbered
   private stream with RESEND and cancel-on-disconnect, authentication/TLS/limits, operations).</details>
5. You edited guide 08. What must you run so an AI using `FULL-CONTEXT.md` sees the change?
   <details><summary>Answer</summary>`java docs/understanding/build-full-context.java` from the repository root.</details>
