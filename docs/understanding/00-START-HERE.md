# 00 — Start Here

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** what the project is, who these guides are for, reading order and study time, how to study them with an AI, the labelling convention | **Prerequisites:** none | **Related:** 01, 13, 14, 15
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

ExchangeServer2 is a small but complete **trading exchange for one instrument**, written in plain Java 17 with no
runtime dependencies. Clients connect over TCP and type text lines such as `NEW a1 SELL LIMIT 10 100` ("sell 10
at 100, my id for this order is a1"). The exchange keeps an **order book** (all buy and sell orders that are
waiting), matches incoming orders against it by **price-time priority** (best price first; at the same price,
first come first served), and tells everyone involved what happened.

Behind that simple surface sit the design problems real exchanges have to solve, which is the point of the
project (Vision §1, **[Verified]** `docs/ExchangeServer2_Vision.md`):

- **Durability:** every accepted command is written to a **journal** (an append-only text file) and forced to
  disk with `fsync` *before* it has any effect. This is called **write-ahead logging**.
- **Recovery:** after a crash, the book is rebuilt by **replaying** the journal through the same matching code.
- **Determinism:** for replay to rebuild the identical book, the matching engine must depend only on its input
  commands: no clock, no random numbers, no counters of its own.
- **Concurrency:** exactly one thread (the **writer thread**, named `exchange-writer`) touches the book; every
  other thread hands requests to it through one queue.
- **Networking:** a thread-per-connection TCP gateway that never lets a slow client stall the exchange.

The code was written entirely by an AI coding agent on 2026-10-27 at the owner's request, as a rewrite of the
owner's earlier hand-written project (**[Verified]** Vision §3, `CLAUDE.md`). The owner is the intended reader of
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
| 01 | big-picture | The problem, features, architecture diagram, the 10 things to know first | 40 min |
| 02 | tech-stack | Java 17 features, concurrency and NIO pieces, Maven/JUnit/JMH/PIT, as used here | 60 min |
| 03 | codebase-map | Every file and what it is for; conventions | 30 min |
| 04 | running-and-tooling | Build, run, test, debug, with real output | 40 min (+30 min doing it) |
| 05 | core-flows | Six end-to-end traces with sequence diagrams | 2–3 h |
| 06 | data-and-state | Data models, journal format, wire protocol, where state lives | 60 min |
| 07 | modules/domain-and-commands | `Rules`, `OrderRequest`, `Order`, `Sequencer`, commands | 45 min |
| 07 | modules/book | `OrderBook`, `PriceLevel`, linked lists with sentinels, invariants | 60 min |
| 07 | modules/engine | `MatchingEngine`: order types, risk checks, outputs | 75 min |
| 07 | modules/journal | `CommandJournal`, `JournalCodec`: fsync, torn tail, lock, fail-stop | 60 min |
| 07 | modules/exchange-and-replay | `Exchange`, `Replay`: the live path, poison pills | 60 min |
| 07 | modules/exchange-service | `ExchangeService`: writer thread, queue, batching, shutdown | 75 min |
| 07 | modules/gateway | `TcpGateway`, `ClientSession`, `Protocol` | 75 min |
| 07 | modules/client-and-main | `ExchangeClient`, `ClientMain`, `Main` | 30 min |
| 08 | cross-cutting | Errors, logging, security, config, concurrency, performance | 45 min |
| 09 | design-decisions | Decision records keyed to the Spec's D-numbers | 60 min |
| 10 | testing | Strategy, seams, coverage, how to write a test | 45 min |
| 11 | risks-and-smells | Ranked honest review | 40 min |
| 12 | change-recipes | Step-by-step "how do I…" | 40 min |
| 13 | glossary | Every term, linked to code | reference |
| 14 | exercises | Self-checks, predict-then-verify, tracing, hands-on, break-it | 4–8 h |
| 15 | open-questions | What could not be determined | 15 min |

Total: roughly 20–25 hours including the exercises. A good three-day plan: day 1 = 00–05; day 2 = 06–07 (all
modules); day 3 = 08–15 with the exercises.

**Minimum path** if you only have two hours: 01 → 05 (flows 1 and 4) → 11.

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
  citation: a repo-relative `path:LINE` (for example `src/main/java/exchangeserver/core/Exchange.java:118`), or a
  design-doc reference such as "Spec D52" (decision 52 in `docs/ExchangeServer2_Spec.md`) or "Spec §5".
- **[Inferred]**: reasoning about intent or behaviour that the code or docs do not state outright. Treat it as a
  well-founded hypothesis; several exercises in guide 14 ask you to confirm or refute one.

Anything that could not be determined is listed in guide 15 (open questions) rather than guessed.

A note on "why": this project is unusual in that most design reasons are **written down** in the Spec as numbered
decisions (D1–D65). Where a guide gives a reason with a D-number, the reason itself is **[Verified]** (it is what
the author recorded); whether the reason is *good* is a separate matter discussed in guides 09 and 11.

## What state of the code these guides describe

- Written **2026-10-27** against git commit `b3091a8` ("Initial Commit", the only commit) **plus uncommitted
  fixes** made the same day while surveying the code (**[Verified]** Spec §10):
  - **D63:** a client session now closes only after every request it sent has been answered (before, `QUIT` right
    after `NEW` lost the reply of an order that had executed).
  - **D64:** opening the journal checks the header before changing the file (before, pointing `--journal` at any
    other file could cut its last line).
  - **D65:** a journal is locked while open, through an exclusive lock on a sibling `<journal>.lock` file (before, `--quarantine` could append to a journal that a running
    exchange was writing).
  - A flaky test (`ExchangeServiceTest$Batching.waitingRequestsShareOneBatch`, 1 failure in the first full run)
    was fixed by reading the writer's statistics only after `stop()`.
  - `Main --quarantine` now reports a refusal in one line with exit code 1 instead of a stack trace.
- Test status after the fixes: **334 tests, all green** (**[Verified]**, seen running; before the fixes the suite
  had 326 tests and one intermittent failure).
- If you are reading this after further commits, check `git log` and the Spec's decision table: anything past D65
  is newer than these guides.

## Key takeaways

- ExchangeServer2 is a one-instrument exchange: TCP gateway → one queue → one writer thread → journal (fsync) →
  deterministic matching engine → order book.
- Its purpose is learning: every design choice is meant to be justified, and the justifications are recorded as
  D-numbers in `docs/ExchangeServer2_Spec.md`.
- Read 01 → 05 first; the module guides (07) go deep once you have the flows.
- **[Verified]** = cited from code/docs or observed running; **[Inferred]** = reasoned. Unknowns live in guide 15.
- The guides describe commit `b3091a8` plus the D63–D65 fixes; 334 tests pass.
- With an AI: paste `AI-TUTOR-PROMPT.md`, then a guide or `FULL-CONTEXT.md`; regenerate the latter with
  `java docs/understanding/build-full-context.java`.

## Common confusions

- **"The exchange" means three different classes.** `Exchange` is the single-threaded core (stamp, journal,
  process); `ExchangeService` wraps it with the writer thread and queue; `Main` starts the whole process. Guide 03
  has the full map.
- **"Order id" vs "client order id".** The order id is a number the exchange assigns (1, 2, 3, …); the client order
  id (`clOrdId`, e.g. `a1`) is chosen by the client and must be unique per participant forever.
- **"Sequence number" is not "order id".** Every journalled command (new order, cancel, skip) gets the next
  sequence number; only new orders get an order id. So after a cancel, the two counters drift apart.
- **The Vision and the Spec are different documents.** Vision = purpose, principles, roadmap. Spec = exact code
  state and decisions. These guides explain both but neither replaces them.

## Questions to test yourself

1. Name the five design problems this project exists to practise.
   <details><summary>Answer</summary>Durability (write-ahead journal with fsync), recovery (replay), determinism
   (engine depends only on its inputs), concurrency (single writer thread fed by a queue), networking (a TCP gateway
   that a slow client cannot stall).</details>
2. A guide says "**[Verified]** Spec D52". What does that tell you, and what does it not tell you?
   <details><summary>Answer</summary>That the stated reason is what the author recorded as decision 52 in
   `docs/ExchangeServer2_Spec.md`. It does not tell you the reason is correct or still current; guides 09 and 11
   judge that separately.</details>
3. Which three behaviours changed on 2026-10-27 after the initial commit?
   <details><summary>Answer</summary>D63 (sessions close only after their requests are answered), D64 (journal
   header checked before the file is modified), D65 (a journal is locked while open, via a `<journal>.lock` file). Plus a flaky-test fix
   and a cleaner `--quarantine` error message.</details>
4. You edited guide 08. What must you run so an AI using `FULL-CONTEXT.md` sees the change?
   <details><summary>Answer</summary>`java docs/understanding/build-full-context.java` from the repository root.</details>
