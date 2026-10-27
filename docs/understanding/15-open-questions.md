# 15 — Open Questions

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** what could not be determined from the repository, ambiguous intent, and the questions you should be able to answer after studying | **Prerequisites:** ideally all guides; at least 01, 05, 09, 11 | **Related:** 09, 11, 12, 14
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

This project documents its intent unusually well: the Spec records decisions D1–D65 with reasons, and open threads in §8. So most "why" questions have a **[Verified]** answer in the Spec. What remains falls into three groups:

1. **Questions for the user**: choices only the owner can make (scope, policy, priorities). They are not bugs; the code is consistent with either answer.
2. **Things this survey could not verify**: behaviour on other platforms, numbers not re-measured, documents outside the repo.
3. **Questions you should be able to answer after studying**: a self-test of understanding, with pointers to the guides but no answers.

Context: the code was written by an AI agent at the user's request on 2026-10-27 (Vision §3). On the same day, the survey for these guides found and fixed four issues (Spec §10, D63–D65 and a flaky test).

---

## 1. Questions for the user (policy and scope)

### Security and robustness (deferred on 2026-10-27)
A security review on 2026-10-27 found four gateway issues, and the user chose to defer all of them. None are recorded in the Spec. **[Verified]** from the project's memory notes, not from the repo.
1. **Network exposure and identity.** The gateway binds to all interfaces (`new InetSocketAddress(port)`, `src/main/java/exchangeserver/gateway/TcpGateway.java:62-68`) and `LOGIN` has no authentication (`gateway/ClientSession.java:225-243`). Anyone on the LAN can log in as any participant, trade, cancel their orders and read their maker fills. *Do these stay deferred? Should the default bind be `127.0.0.1` with a `--bind` option (the cheapest fix)?* Vision §6 lists "authentication; TLS" as non-goals, but the memory notes say that wording was the AI's addition in Vision v4, not the user's.
2. **Memory-exhaustion flood.** Participant names are self-chosen, so the per-participant open-order cap (10,000) does not bound the total. The client-order-id map only grows (`engine/MatchingEngine.java:43`; Spec D49 cost). There is no rate limit. An OOM is an `Error`, which halts the writer, and replay rebuilds the same state, so restarts can fail too **[Inferred]**. *Accept for a learning project, or add a global resting-order cap and per-session rate limit?*
3. **No timeouts.** Sockets have no read, login or idle timeout, so 256 idle pre-login connections exhaust the session slots (`TcpGateway.java:109-115`). *Wanted?*
4. **No fairness.** One global queue with a capacity of 100,000 means one pipelining client can make everyone get `BUSY` (`core/ExchangeService.java:182-202`). *Per-session in-flight limits?*

### Trading policy
5. **Cancel-on-disconnect.** Resting orders stay on the book after a session closes (Spec §6, as of D63). Many venues offer cancel-on-disconnect. *Wanted, or keep orders (the current behaviour)?* If wanted, a disconnect becomes a journalled command (determinism), so it needs a design decision.
6. **Self-trade prevention.** Self-trades are allowed (Spec §8; Vision §7 "next candidates" #3 needs a policy: cancel newest / oldest / both). *Which policy, and when?*
7. **Open-order cap on a fully fillable LIMIT.** The cap is checked on arrival, so a participant at 10,000 resting orders cannot send a LIMIT that would fill completely and never rest (`MatchingEngine.java:118-121`; Spec §8 "simple and deterministic; real venues differ"). *Intended long term, or a placeholder?*
8. **Client-order-id lifetime.** Unique per participant *forever* (D49). The Spec's planned fix is a trading day, ended by snapshot + fresh journal. *When does "a trading day" get built (Vision §7 next candidate #1)?*

### Operability
9. **Snapshots and journal rotation.** Recovery replays the whole journal, O(file size) (Spec §8). Crossing a rule change needs "a snapshot + fresh journal", which the startup error message recommends even though snapshots don't exist (`journal/CommandJournal.java:263-264`). *Priority relative to the other next candidates?*
10. **Startup reporting.** `Main` prints neither how many commands were replayed nor how many torn bytes were cut. `Exchange.replayedOnOpen()` and `truncatedBytesOnOpen()` exist, but only tests read them **[Verified]** (grep; Spec §8). *Should a silent torn-tail cut be visible to the operator?*
11. **Writer stats lag replies.** `batches()`, `processed()` and `largestBatch()` are updated after a batch's futures complete (`core/ExchangeService.java:101-106,259-263`). Only tests and the benchmark read them today. *Does anyone need them to be exact at reply time (e.g. a future metrics endpoint)?*
12. **CRLF tolerance.** Records are LF-only. A journal converted to CRLF (an editor, or git `autocrlf` if a journal were ever committed) fails at startup on its header (observed 2026-10-27; Spec §8). *Tolerate a trailing `\r` (a format decision, `FORMAT_VERSION`?), or keep strict?*
13. **Double SKIP.** `--quarantine` accepts a command that is already skipped and appends a second, harmless SKIP (observed 2026-10-27; `core/Exchange.java:94-114` checks only that the target exists and is not itself a SKIP). *Refuse it for clarity?*
14. **Generic startup hint.** Every startup failure prints the poison-pill hint, even a rules-version mismatch (`Main.java:64-69`). *Tailor the message?*

### Documentation and design
15. **Original-project documents.** Code comments cite "threads guide 4.3 / 4.4 / Lab 6" and "Spec v8" / "v3". These live in the original project (`Exchange Server/New Learnings/03_Threads_Visibility_and_Handoff.md`, `Exchange Server/src/docs/ExchangeServer_Spec_v8.md`), and Spec §1.6 now names them. *Copy them into this repo, or keep pointing outside?*
16. **D63's barrier.** A session waits for its replies by queueing a depth-1 snapshot read (`gateway/ClientSession.java:143-153`). The trade-off is recorded in Spec D63: it reuses the existing request type, at the cost of one tiny snapshot per disconnect. *Promote it to a dedicated `Request.Barrier` in `core` if more barrier uses appear?*
17. **Collaboration mode for future work.** `CLAUDE.md` and Vision §3 say to ask, per piece of work, whether the user writes the code (with the LLM reviewing) or the LLM writes it. The fixes in Spec §10 were LLM-written on the user's instruction "Fix first". *Which mode for the next roadmap step?*

---

## 2. Not verified by this survey

18. **Journal lock on Linux/macOS: design hole resolved in-session; POSIX behaviour still untested.**
    - *The hole.* D65's first version locked one byte of the journal file itself. The `java.nio.channels.FileLock` Javadoc warns that "on some systems, closing a channel releases all locks held by the Java virtual machine on the underlying file regardless of whether the locks were acquired via that channel or via another channel" (general Java knowledge). POSIX `fcntl` locks behave this way, and `read()`, the header check and the torn-tail cut all open and close their own handles on the journal. So on Linux/macOS that lock would have vanished right after `open()`.
    - *The fix, made on 2026-10-27 **[Verified]**:* the lock now lives on a separate sibling file, `<journal>.lock`, that nothing else ever opens. Details:
      - `Files.createFile` (create-new) never opens an existing lock file, so it never closes one.
      - A whole-file `tryLock` on its own channel.
      - Key = `lockFile.toRealPath()`.
      - A static in-JVM `HELD` set refuses a second open in the same JVM before it touches the lock file.
      - The lock is acquired before the journal channel opens; `close()` closes the journal channel, then releases the lock.
      - The file is left on disk (git-ignored `*.journal.lock`).

      Code: `journal/CommandJournal.java:52-53,71-103,185-249`. The regression test `CommandJournalTest$Exclusive.anotherProcessIsRefusedWhileTheJournalIsOpen` (`src/test/java/exchangeserver/journal/CommandJournalTest.java:296-312`) appends and calls `read()` (closing a handle on the journal) and only then runs `Main --quarantine` in a child JVM, expecting exit 1 and "already open". It passes on Windows.
    - *Still open:* nobody has run it on Linux or macOS. Docker Desktop exists on the dev machine but was not started. To check: `docker run --rm -v "$PWD":/w -w /w eclipse-temurin:17 ./mvnw -B test -Dtest=CommandJournalTest` (exercise d7 in guide 14). Smaller open points **[Inferred]**:
      - A stale lock file is harmless (it is reused), but on a network file system lock semantics may differ.
      - The lock file sits next to the journal, so the journal's directory must be writable even for `--quarantine`.
19. **Directory fsync after creating the journal.** Not done; this matters on Linux (Spec §8). Unverified whether a new journal can vanish after a crash on ext4.
20. **Benchmark numbers.** The Spec §5 tables (engine ~0.65–1.1 µs/command; smart batching 43 orders/ms at 64 clients) were not re-run for these guides. The D52 and D59 decisions rest on them.
21. **Clean Ctrl+C shutdown.** Only hard kills were exercised live. The clean path (shutdown hook → `gateway.close()` → `service.stop(10 s)`, `Main.java:74-83`) is covered by unit tests of `stop()`, not by a run of `Main`.
22. **Mutation results after the fixes.** The PIT re-run results for D63–D65 are recorded in Spec §7 by the session that made the fixes. Check there for the current survivor list.
23. **Linger mode on Linux.** The Spec explains the poor linger numbers by the Windows ~15.6 ms timer resolution. Not measured on another OS.
24. **Exit code after Ctrl+C.** `Main`'s header comment and the README say exit code 0 after a clean stop (`Main.java:19`). But Ctrl+C makes the JVM run its shutdown hooks *as part of exiting on the signal*. When the JVM exits because of SIGINT, the process exit status is normally 130 (128 + signal 2) on Linux/macOS. On Windows a console Ctrl+C typically gives a non-zero status too (e.g. 130 or `-1073741510` / `0xC000013A`, depending on the shell). `main` returning normally does not set the code, because the exit is already under way. **[Inferred, unverified]**: run the jar, press Ctrl+C, and check `$?` / `$LASTEXITCODE`. If it is not 0, either the documentation is wrong, or `Main` should call `System.exit(0)` itself after a clean stop (which it cannot do from inside a shutdown hook, since `exit` blocks there). Either way, scripts that treat "non-zero = halted" would misread a clean Ctrl+C; only exit code 2 means halted.

---

## 3. Questions you should be able to answer after studying

No answers here; each points to where the answer lives. If you can answer all of them without looking, you understand the project the way its author does.

**Architecture and flow**
1. Trace a crossing NEW from the socket to the maker's FILL line, naming every thread and every class it touches. *(guide 05 flow 1)*
2. Why does sequence order equal processing order without any lock around the `Sequencer`? *(07 exchange-service; Spec D55)*
3. Why must the reply callback run before the `OutputListener` for the same command, and what guarantees it? *(07 exchange-service; 07 gateway; D53)*
4. What exactly does the caller's future protect against that the writer's own future did not? *(07 exchange-service; D61)*

**Correctness and invariants**
5. List the book invariants and the single method responsible for each. *(07 book)*
6. Why is `fillBest` in `OrderBook`, not in the engine? What would break if the engine could unlink nodes? *(07 book; D44)*
7. Why is a market order's collar fixed at arrival, and how do floor and ceil make it agree with `withinBand`? *(07 engine; D50)*
8. Why can't a FOK use the normal matching loop and "undo" on failure? *(07 engine)*

**Durability and recovery**
9. State "visible ⇒ durable" and "durable ⇒ visible". Which one holds, why, and what is the client's remedy for the other? *(07 journal; Spec §4; D49)*
10. Why is a torn last record cut but a complete corrupt record never dropped? *(07 journal; D38)*
11. Why is a poison pill skipped automatically when it happens live, but never at startup? *(07 exchange-and-replay; D51)*
12. After a live poison pill in the middle of a batch, why do the later commands in that batch run on a *rebuilt* engine, and why is that consistent with replay? *(05 flow 5)*
13. Why does `open` lock, then check the header, then cut — and what went wrong when it cut first? *(07 journal; D64, D65)*

**Concurrency**
14. Why does no field of `Pending` need `volatile`? *(07 exchange-and-replay; happens-before)*
15. How do `stop()` and `enqueue()` together guarantee "refused or processed, never lost"? *(07 exchange-service; D56)*
16. Why may a test read `processed()` only after `stop()`? *(10; Spec §10 #1)*
17. How does the subscription number prevent two BOOK lines after SUBSCRIBE, UNSUBSCRIBE, SUBSCRIBE? *(07 gateway; Spec §9 #3)*

**Design judgement**
18. Why was the cached best-level pointer rejected? What measurement decided it? *(09; D59; Spec §5)*
19. Why smart batching instead of linger, and why did linger measure so badly on Windows? *(09; D52)*
20. Which changes need `Rules.VERSION` bumped, and which need `FORMAT_VERSION`? *(12)*
21. Name three risks in guide 11 that tests cannot catch, and say how you would detect each in production. *(11; 14 (e))*

---

## Key takeaways

- Most "why" questions are answered by a D-number in the Spec. The open questions are about policy and scope, not mystery code.
- The four deferred security findings are the biggest open decisions. They are recorded only in the project's memory notes, not in the Spec.
- Cross-platform behaviour (the journal lock, the directory fsync, linger timing) is verified on Windows only.
- Benchmarks were not re-run; decisions D52 and D59 rest on the Spec §5 numbers.
- If you can answer section 3 without the guides, you are ready to change the code with confidence.

## Common confusions

- **"An open question means a bug."** Mostly not. Items 5–8 are deliberate simplifications that the Spec documents.
- **"Non-goals in the Vision were the user's decisions."** The memory notes say the "authentication; TLS" non-goal wording was added by the AI in Vision v4. Confirm with the user.
- **"D63–D65 were part of the original design."** They were added on 2026-10-27, during the survey for these guides (Spec §10).

## Questions to test yourself

1. Which open question would change the journal format if answered "yes"?
<details><summary>Answer</summary>CRLF tolerance (12) arguably needs a format decision. So do cancel-on-disconnect (5), which needs a new journalled command type, and trading-day scoping (8), which needs snapshots and rotation. A new command type means a new record word in <code>JournalCodec</code>.</details>

2. Why can't cancel-on-disconnect simply be done by the gateway calling `book.cancel` when a socket closes?
<details><summary>Answer</summary>Two reasons. Only the writer thread may touch the book (single writer). And every state change must be a journalled command, or replay would rebuild a different book. The gateway would have to submit cancel requests through <code>ExchangeService</code>, or a new "disconnect" command, stamped and journalled.</details>

3. What could go wrong with the journal lock on Linux that cannot happen on Windows?
<details><summary>Answer</summary>POSIX record locks belong to the process, and closing <em>any</em> descriptor for the file releases them (the <code>FileLock</code> Javadoc warns about this); on Windows, locks belong to the handle. A lock on the journal itself would therefore vanish on Linux as soon as <code>read()</code>, the header check or the torn-tail cut closed its handle. That is why D65 now locks a separate <code>&lt;journal&gt;.lock</code> file that nothing else opens or closes, plus an in-JVM <code>HELD</code> set. Whether the fix holds on Linux has not been run yet (item 18).</details>
