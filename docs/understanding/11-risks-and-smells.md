# 11 — Risks and smells

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** an honest review: bugs and likely bugs, risks, fragile spots, tech debt and code smells, ranked by severity, plus what was fixed during the survey | **Prerequisites:** 01, 05, 08 | **Related:** 09, 10, 12, 15
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

This is the "what could hurt you" guide. It is honest rather than polite. Overall, the code is careful: invariants are enforced at their source, every decision is documented, the tests are strong, and a 2026-10-27 survey found only a handful of real defects (fixed, §6). What remains is mostly **deliberate scope limits** (no security, no snapshots) and **operational fragility**.

**Severity scale** (for *this* project: a single-machine learning system, not a production venue):
- **High**: could lose data, wedge the system or let an outsider act as a participant under realistic use (e.g. on a shared network).
- **Medium**: wrong or surprising behaviour in an edge case, or a gap that will bite as the project grows.
- **Low**: misleading output, test fragility, minor inefficiency.
- **Smell**: not a bug; makes the code harder to read or change.

Each entry gives **location · evidence · impact · label · suggested direction**. The direction is a pointer, not a fix: whether and how to fix is the user's call (guide 15 lists the open decisions). None of the High items are fixed: the four security findings were **deferred by the user on 2026-10-27**, and the rest are documented open threads in Spec §8.

There are **no Critical items** for the intended use (local, single user). Items H1 and H2 would become Critical the moment the gateway is reachable by untrusted machines.

---

## 1. High

### H1. Unauthenticated gateway listening on every network interface
- **Location:** `src/main/java/exchangeserver/gateway/TcpGateway.java:67`; `ClientSession.login` `src/main/java/exchangeserver/gateway/ClientSession.java:225-243`.
- **Evidence:** `server.bind(new InetSocketAddress(port))` binds all interfaces; `LOGIN <name>` accepts any name matching `[A-Za-z0-9_]{1,16}`, with no secret and no check that the name is in use [Verified].
  ```java
  // src/main/java/exchangeserver/gateway/TcpGateway.java:62-68
  public TcpGateway(int port, int maxSessions, int outboundCapacity) throws IOException {
      this.maxSessions = maxSessions;
      this.outboundCapacity = outboundCapacity;
      this.server = new ServerSocket();
      server.setReuseAddress(true);
      server.bind(new InetSocketAddress(port));     // all interfaces
  }
  ```
- **Impact:** anyone who can reach the port can log in as any participant, trade in their name, cancel their orders and receive their maker fills (passive fills go to *every* session of that participant). No TLS, so everything is readable on the wire [Verified].
- **Status:** deferred finding 1 (user, 2026-10-27); "authentication; TLS" is a Vision §6 non-goal (wording added by the AI in Vision v4, per the user's memory notes) [Verified].
- **Direction:** bind to `127.0.0.1` by default with a `--bind` flag (the cheapest fix noted); later, per-participant credentials.

### H2. Memory exhaustion that replay reproduces
- **Location:** client-order-id map `src/main/java/exchangeserver/engine/MatchingEngine.java:43, 110-114`; open-order cap keyed by self-chosen names `src/main/java/exchangeserver/engine/MatchingEngine.java:118-121`; no rate limit anywhere.
- **Evidence:** every new order adds a `(participant, clientOrderId) → orderId` entry that is never removed (D49). The cap of 10,000 resting orders is per participant, and participant names are free, so a client can rest unlimited orders by logging in under new names [Verified].
- **Impact:** a flood grows the heap until an `OutOfMemoryError`. That is an `Error`, so the writer **halts** (not a poison-pill quarantine), and the process exits 2. On restart, replay rebuilds the same map and book from the journal, so the restart may hit the same OOM: the exchange can be bricked until someone edits or truncates history [Verified for halt-on-Error; the restart loop is Inferred].
- **Status:** deferred finding 2 [Verified — user memory].
- **Direction:** a global cap on resting orders, a rate limit per session, authenticated participants (H1); for the map, trading-day scoping (H3).

### H3. History only grows: no snapshots, no rotation, unbounded id map
- **Location:** Spec §8 "Client-order-id map only grows", "No snapshots"; `Replay` reads the whole journal on every start `src/main/java/exchangeserver/core/Replay.java:36-43`.
- **Evidence:** recovery is O(journal size), streamed twice (verify, then feed); memory for the id map is O(all orders ever) [Verified].
- **Impact:** startup time and memory grow without bound. A poison-pill rebuild also replays everything before the poison, so it gets slower over time too. Crossing a `Rules.VERSION` change is impossible without losing the book, because "snapshot + fresh journal" is not built [Verified — Spec §8, D46].
- **Direction:** the Vision's first "next candidate": snapshots + journal rotation, with client order ids scoped to a trading day.

---

## 2. Medium

### M1. No socket timeouts (deferred finding 3)
- **Location:** `ClientSession.readLoop` / `readLine`; no `setSoTimeout` anywhere in `src/main` [Verified].
- **Impact:** a connection that never sends anything holds two threads forever. 256 idle, never-logged-in connections exhaust the session limit, and further clients get `ERROR too many sessions` [Verified for the limit; the exhaustion scenario is Inferred].
- **Direction:** a login timeout and an idle timeout (read timeout + PING expectations).

### M2. No fairness between clients (deferred finding 4)
- **Location:** one global capacity check `src/main/java/exchangeserver/core/ExchangeService.java:192-193`.
- **Impact:** one client pipelining requests can fill the 100,000-slot queue, and then *every* client gets `BUSY` [Verified].
- **Direction:** a per-session in-flight limit at the gateway.

### M3. Durable ⇒ visible gap on halt or crash
- **Location:** `ExchangeService.halt` `src/main/java/exchangeserver/core/ExchangeService.java:302-319`; Spec §4.
- **Evidence:** the batch is fsynced *before* processing. If the process dies (or halts) after the fsync, journalled commands whose callers were told `HALTED`, or nothing at all, are replayed on restart [Verified — Spec §4, §8].
- **Impact:** a client can believe an order failed while it actually rests on the book after restart. The documented remedy is client-side: resend with the same client order id and read the `DUPLICATE_CLIENT_ORDER_ID` reply, which carries the original id (D49). Nothing forces clients to do this, and there is no resend of missed fills [Verified].
- **Direction:** output sequence numbers + resend (Vision §7 candidate 2).

### M4. D65's lock is untested where it matters most
- **Location:** `CommandJournal.Lock` `src/main/java/exchangeserver/journal/CommandJournal.java:198-252`.
- **Evidence:** the lock was redesigned on 2026-10-27 to use a separate `<journal>.lock` file, because a lock on the journal itself would be dropped on Linux/macOS whenever any handle to the journal closes (POSIX semantics). The cross-process test (`CommandJournalTest.Exclusive.anotherProcessIsRefusedWhileTheJournalIsOpen`) has only ever run on **Windows** [Verified — Spec D65].
- **Impact:** on Linux/macOS the protection against running `--quarantine` next to a live exchange (which corrupts sequence numbers) is reasoned, not demonstrated. Locks on network filesystems are unreliable in general [Inferred].
- **Direction:** run the suite on Linux/macOS (CI would do it).

### M5. `Rules.VERSION` does not catch behaviour changes in code
- **Location:** `src/main/java/exchangeserver/domain/Rules.java:5-16`; Spec §8.
- **Evidence:** the journal header records only the rules version. Any code change that alters matching (for example a different check order, rounding, or a new order-type rule outside `Rules`) replays old commands differently and **silently** builds a different book [Verified — Spec §8].
- **Impact:** the core guarantee ("restart rebuilds the identical book") depends on discipline. Nothing detects divergence: no checksums of state, no recorded outputs to compare against [Verified/Inferred].
- **Direction:** record a state digest (e.g. hash of the book) periodically in the journal and verify it during replay; or snapshot + fresh journal on every behavioural release.

### M6. Journal integrity: no per-record checksum, directory not fsynced
- **Location:** format `src/main/java/exchangeserver/journal/JournalCodec.java:16-30`; `CommandJournal.open` creates the file without fsyncing its directory; Spec §8.
- **Impact:** a bit flip that still parses changes history silently. On Linux, a crash right after creating a new journal can lose the file's directory entry [Verified — Spec §8; the Linux behaviour is general knowledge].
- **Direction:** a CRC per record; fsync the parent directory after creating the file.

### M7. LF-only journal: a CRLF conversion bricks startup
- **Location:** `JournalCodec.decodeHeader` / `decode` split on `,` and keep any trailing `\r` `src/main/java/exchangeserver/journal/JournalCodec.java:51-66, 101-126`.
- **Evidence:** observed live on 2026-10-27: a journal rewritten with Windows line endings fails at startup with `Bad journal header "HEADER,2,9\r": For input string: "9\r"` [Verified].
- **Impact:** opening the journal in a Windows editor, or committing it to git with `autocrlf`, makes the exchange refuse to start. The data is intact but needs manual conversion. (`*.journal` is gitignored, which avoids the git case by default.)
- **Direction:** decide whether to tolerate `\r\n` (a format decision, guide 15) or detect it and print a specific message.

### M8. Disconnect leaves orders resting, and missed fills are lost
- **Location:** `ClientSession.close` → `TcpGateway.removed` `src/main/java/exchangeserver/gateway/TcpGateway.java:154-165`; routing in `onOutput` `src/main/java/exchangeserver/gateway/TcpGateway.java:183-207`.
- **Evidence:** there is no cancel-on-disconnect, and a passive fill for a participant with no open session is dropped (Spec §6, §8) [Verified].
- **Impact:** a client that disconnects keeps market risk on the book and cannot learn what traded while it was away, except by inference from a fresh `SUBSCRIBE` snapshot.
- **Direction:** an optional cancel-on-disconnect; output sequence numbers + resend.

### M9. Exceptions in callbacks vanish silently
- **Location:** `pending.result.whenComplete(onDone)` `src/main/java/exchangeserver/core/ExchangeService.java:183-185`.
- **Evidence:** `CompletableFuture.whenComplete` returns a new future that captures an exception thrown by the callback; nobody looks at that future [Inferred from `whenComplete` semantics; no test covers a throwing callback — Verified by grep].
- **Impact:** a bug in a gateway callback (e.g. a formatting exception in `Protocol.order`) would mean the client silently gets no reply, with nothing logged. The listener path, by contrast, logs (`Exchange.java:162-163`).
- **Direction:** wrap callbacks so a throw is logged.

### M10. Policy edges that may surprise
- **Self-trades are allowed** (no self-trade prevention) [Verified — Spec §8].
- **The open-order cap is checked on arrival**, so a LIMIT that would *fully fill* is rejected when the participant is at the cap [Verified — `src/main/java/exchangeserver/engine/MatchingEngine.java:118-121`; Spec §8].
- **A rejected order still uses up its client order id**, including band and cap rejects [Verified — `src/main/java/exchangeserver/engine/MatchingEngine.java:110-114`].
- **No band before the first trade**: the first market order can sweep the whole book [Verified — `src/main/java/exchangeserver/engine/MatchingEngine.java:187-189`].
- **During shutdown, D63 does not wait**: a session's close-after-replies read is refused once stopping, so it closes at once [Verified — `src/main/java/exchangeserver/gateway/ClientSession.java:143-145`].
- **Direction:** user decisions (guide 15).

---

## 3. Low

### L1. Startup says nothing about what recovery did
- **Location:** `src/main/java/exchangeserver/Main.java:71-72`; `Exchange.replayedOnOpen()` / `truncatedBytesOnOpen()` `src/main/java/exchangeserver/core/Exchange.java:224-230`.
- **Evidence:** in the live crash-recovery run, a 20-byte torn record was cut and the server printed only "listening on port ..." [Verified].
- **Impact:** an operator can't tell that data was cut or how much was replayed.
- **Direction:** print replayed count, truncated bytes and quarantined set at startup.

### L2. Misleading operator messages
- **Location:** `src/main/java/exchangeserver/journal/CommandJournal.java:263-264`; `src/main/java/exchangeserver/Main.java:66-67`.
- **Evidence:** the rules-version refusal says "Start a fresh journal from a snapshot", but snapshots don't exist. And **every** startup failure prints "If replay failed on one command (a poison pill), inspect it, then restart with --quarantine <seq>", including a version mismatch or a non-journal file, where quarantining is wrong [Verified — observed live].
- **Direction:** print the poison-pill hint only for replay failures on a record; change the snapshot advice to what is actually possible today.

### L3. `--quarantine` accepts an already-skipped command
- **Location:** `Exchange.quarantine` `src/main/java/exchangeserver/core/Exchange.java:96-114` refuses a SKIP *record* but not a command that already has a SKIP.
- **Evidence:** observed live: quarantining 7 twice appended `SKIP,8,...,7` and `SKIP,9,...,7` [Verified].
- **Impact:** harmless (replay ignores the duplicate), just noise in history [Verified — Spec §8].

### L4. `ExchangeClient.linesUntil` can drop duplicate lines
- **Location:** `src/main/java/exchangeserver/client/ExchangeClient.java:86-97`.
- **Evidence:** `skipped.removeAll(lines)` removes every equal string, not just the ones this call consumed [Verified]. A line equal to one in `lines` but skipped and not yet consumed (e.g. a second `PONG`) disappears.
- **Impact:** test-helper flakiness only; `ExchangeClient` is in `src/main` because `ClientMain` and the tests share it [Inferred].

### L5. Test fragility
- A spin-wait without a deadline (`racingStopNeverLosesARequest`, `src/test/java/exchangeserver/core/ExchangeServiceTest.java:348-350`) can hang instead of failing [Verified].
- `Thread.sleep(50)` shapes a linger batch (`src/test/java/exchangeserver/core/ExchangeServiceTest.java:525`); allowed by the Spec, but timing-dependent [Verified].
- The D63 regression test reproduces the old bug probabilistically (200 requests racing one `QUIT`) [Verified/Inferred].
- See guide 10 §9.

### L6. Small operational edges
- `stop()` called from the writer thread (e.g. inside a listener) waits for its own end until the timeout [Verified — Spec §8].
- The gateway binds its port **before** replay, and the acceptor starts after it. During a long replay, clients can connect (the OS queues them in the backlog) but get no response until replay ends [Verified — `src/main/java/exchangeserver/Main.java:60-71`; the client experience is Inferred].
- The acceptor retries a failing `accept()` every 100 ms forever, with a warning each time and no alarm [Verified — Spec §8].
- The lock file `<journal>.lock` is left behind after close by design (deleting it would race with the next opener); it is empty and gitignored [Verified — `src/main/java/exchangeserver/journal/CommandJournal.java:205`; `.gitignore:6`].

---

## 4. Smells (not bugs)

| Smell | Location | Comment |
|---|---|---|
| An instance field named like a constant: `private final Pending<Void> STOP` | `src/main/java/exchangeserver/core/ExchangeService.java:91` | A per-instance marker compared by identity; `UPPER_CASE` suggests `static`. Works correctly [Verified] |
| Unchecked cast to complete a future | `Pending.complete(Object)` `src/main/java/exchangeserver/core/Pending.java:27-30` | `@SuppressWarnings("unchecked")`; type safety rests on `process` pairing each request with the right output type [Verified] |
| `instanceof` chains instead of `switch` over sealed types | `Exchange.process` `src/main/java/exchangeserver/core/Exchange.java:122-126`, `Replay.feed`, `JournalCodec.encode`, `ClientSession.handle` | **Justified** on Java 17: pattern-matching `switch` was only a preview feature there (final in 21). The cost: the compiler doesn't check exhaustiveness for these chains (except where a `switch` over an enum is used, as in `MatchingEngine.process`) [Verified/Inferred] |
| Benchmark-only mode in production code | `lingerNanos` / `linger()` `src/main/java/exchangeserver/core/ExchangeService.java:255-257, 286-299` | Kept "only so the benchmark can compare" (comment, D52). Its boundaries are equivalent-mutant survivors [Verified] |
| Convenience API on a production class used by tests and tools | `Exchange.submit/cancel/snapshot` `src/main/java/exchangeserver/core/Exchange.java:191-216` | Labelled "single-threaded convenience API (tools, tests, the benchmark baseline)"; a second way to drive `Exchange` that production never uses [Verified] |
| Unused method | `CommandJournal.getFile()` `src/main/java/exchangeserver/journal/CommandJournal.java:181-183` | No caller anywhere; PIT reports no coverage [Verified] |
| Comments coupled to external documents and decision numbers | e.g. `(threads guide 4.3)` `src/main/java/exchangeserver/core/ExchangeService.java:35`, `(threads guide, Lab 6)` `src/main/java/exchangeserver/core/Pending.java:10`, `Spec v8` references, D-numbers everywhere | Useful as a trail, but the "threads guide" and "Spec v8" live in a *different* project folder; Spec §1.6 now names the files. Without the Spec at hand, many comments read as cryptic [Verified] |
| Defensive double checks | `remaining > 0` checked in both `OrderBook.add` (`src/main/java/exchangeserver/book/OrderBook.java:52`) and `PriceLevel.addOrder` (`src/main/java/exchangeserver/book/PriceLevel.java:69`); the FOK "fillable FOK left remaining" throw (`src/main/java/exchangeserver/engine/MatchingEngine.java:164-169`); duplicate-id check in `add` plus the engine's increasing-id guard | Harmless; each layer protects its own invariant ("fail loudly"). They are also why several PIT boundary mutants are equivalent [Verified] |
| Sentinels by identity | `new String("<close>")` `src/main/java/exchangeserver/gateway/ClientSession.java:41-42`; `"\u0000EOF"` in `ExchangeClient` | Deliberate and correct (identity, not equality), but surprising to readers and linters (hence the `@SuppressWarnings`) [Verified] |
| Mutable captured arrays for lambda state | `long[] fed = {0}` (`Replay.feed`), `long[] last`, `boolean[] exists` (`Exchange.quarantine`) | A common Java idiom; a small class or loop would read better [Verified] |

### Traits typical of AI-written code, as observed here

The user asked for these to be called out. Each is a pattern seen in this codebase, not a guess about how it was written [Verified examples]:

1. **Very dense, decision-referencing comments.** Almost every class opens with a block comment, and many lines cite `(D44)`, `(step 4A)`, `(threads guide 4.3)`. The comments are accurate and often explain *why*, which is good. But they assume the reader has the Spec and the original project's documents open, and they can drift from the code without anything noticing.
2. **Confident claims in docs that didn't hold.** The Spec said "326 tests, all green (three consecutive full runs)"; the first run during the survey was 325/326 (flaky test, guide 10 §6). The first D65 lock was described as working, and it did on Windows, but its design was wrong for POSIX systems. CLAUDE.md said "decisions D1–D60" when the Spec had D62, and "not a git repository yet" when it was one. **Lesson: verify the docs' status claims by running things.**
3. **Belt-and-braces defensiveness** (the double checks above) next to real gaps elsewhere (no auth, no timeouts). Effort went into internal invariants more than into the edges where the outside world comes in.
4. **Polished internals with operational blind spots**: startup reports nothing about recovery (L1), operator messages point to features that don't exist (L2).
5. **Self-review fixes that leave fingerprints**: comments like "Review finding 8" in tests, and §9/§10 tables, record each round of fixes. Useful history, but a reader must know the review happened.

---

## 5. Things that look like risks but are fine

- **No locks in the book or engine.** Correct by the single-writer design; locks would add nothing (guide 08 §5).
- **Unbounded request queue.** Capacity is enforced under the hand-in lock, so it is bounded in practice; the queue is unbounded only so `stop()` can always enqueue `STOP` (D56).
- **`HashMap`s in the engine and book.** Only looked up, never iterated, so they don't break determinism; the one iterated collection is a `LinkedHashSet` (guide 08 §7).
- **Torn records are cut on open.** Only bytes after the last `\n` (never a complete record), and only after the header check (D64).
- **Rejected orders consuming ids.** Required for replay (state-dependent rejects are journalled); gaps in ids are expected (D16).

---

## 6. Resolved in this session (2026-10-27)

Found while surveying the code to write these guides, then fixed at the user's request ("fix first"). All are recorded in Spec §10. **All seven regression tests written for items 2–4 were run against the original code (restored from git) and failed there**, then passed on the fixed code [Verified — observed].

| # | Finding | Fix | Regression test(s) |
|---|---|---|---|
| 1 | **Flaky test.** `waitingRequestsShareOneBatch` failed once in a full run (`expected 50 but was 1`): the writer's stats are updated after `process()` completes the batch's futures; five assertions had the race | Tests read stats after `stop()` (the writer's `join` is the happens-before edge); the field comment documents the lag; production behaviour unchanged | the five `ExchangeServiceTest` assertions themselves |
| 2 | **D63.** `QUIT` (or end of stream) right after `NEW` dropped the reply of an order that executed and may rest; against the old code 199 of 200 replies were lost | Close only after a queued read completes behind the session's requests; `BYE` goes last | `TcpGatewayTest.Session.quitAnswersEveryEarlierRequestBeforeBye`, `endOfStreamStillGetsTheReplies` |
| 3 | **D64.** `CommandJournal.open` cut the "torn tail" of *any* file before checking its header: `--journal` pointed at a wrong file lost its last line; a one-line file without a newline was **emptied and turned into a journal**; a journal of other rules was cut before being refused | Verify the header (or that the content is a prefix of a header) before cutting or writing | `CommandJournalTest.Header.fileThatIsNotAJournalIsRefusedUnchanged`, `textWithoutAnyRecordIsRefusedUnlessItStartsAHeader`, `otherRulesVersionWithATornTailIsRefusedUnchanged` |
| 4 | **D65.** Nothing stopped `--quarantine` from appending to a journal a running exchange was writing, interleaving sequence numbers | Exclusive lock while open; `Main --quarantine` now reports a refusal in one line (exit 1) instead of a stack trace. **Reworked the same day**: the first version locked one byte of the journal itself, which works on Windows but would be released on Linux/macOS whenever any handle to the journal closes (POSIX semantics); the lock now lives on a separate `<journal>.lock` file plus an in-JVM `HELD` set | `CommandJournalTest.Exclusive.aSecondOpenIsRefusedUntilTheFirstCloses`, `ExchangeTest.PoisonPill.quarantineToolRefusesWhileTheExchangeIsRunning`; after the rework also `anotherProcessIsRefusedWhileTheJournalIsOpen` (child JVM, Windows only so far, see M4). A lock-release assertion added to `otherRulesVersionIsRefusedAndTheFileIsReleased` also kills a formerly unobservable PIT mutant |
| 5 | Code comments cite a "threads guide" and "Spec v8" that aren't in this repo | Spec §1.6 names the files in the original project (`Exchange Server/New Learnings/03_Threads_Visibility_and_Handoff.md`, `Exchange Server/src/docs/ExchangeServer_Spec_v8.md`) | — |
| 6 | CLAUDE.md drift: "D1–D60", a stale test count, "not a git repository yet" | Updated; CLAUDE.md also warns about stale incremental builds after restoring files | — |

A process lesson from item 4: the first live check of the lock appeared to fail because the jar was **stale**. Restored source files kept older timestamps, so Maven's incremental compiler didn't recompile them. A `clean` build showed the fix working. When a fix "doesn't work", check the build first [Verified].

---

## 7. Top 5

1. **H1 — Anyone on the network can log in as any participant** (gateway binds all interfaces, no authentication, no TLS). Deferred by the user.
2. **H2 — A memory flood halts the exchange, and replay can reproduce the OOM on restart** (self-chosen names bypass the per-participant cap; the client-order-id map never shrinks; no rate limit). Deferred by the user.
3. **H3 — History only grows**: no snapshots or rotation, so recovery time, the id map and poison-pill rebuild cost grow forever, and a rules change can't be crossed without losing the book.
4. **M5 — Replay correctness depends on discipline**: a code change that alters matching silently rebuilds a different book; `Rules.VERSION` only catches deliberate rule-constant changes.
5. **M4 — The journal lock (D65) is untested on Linux/macOS**, the platforms its redesign targets; nothing at all has been run off Windows.

---

## Key takeaways

- No Critical items for local use; **H1–H3** are the real risks, and H1/H2 were consciously deferred by the user.
- The internals (book, engine, journal) are well protected; the risks concentrate at the **edges**: network, operations, long-term growth, platform differences.
- **Replay's "identical book" guarantee** rests on determinism plus discipline about code changes (M5) and journal hygiene (M6, M7).
- Several "surprises" are **policy**, not bugs (self-trades, cap on fully fillable orders, ids consumed by rejects, no cancel-on-disconnect); they need decisions, not fixes.
- The survey found and fixed **four real defects** (flaky stats test, lost replies on QUIT, journal open damaging foreign files, no single-writer lock), each with regression tests proven against the old code.
- **Don't trust status claims in docs without running things**: "326 green" hid a flake, and a correct-looking lock was wrong for POSIX.

## Common confusions

- *"Deferred means unknown."* The four security findings are known and documented in the user's notes; deferring was a deliberate scope choice.
- *"The poison-pill policy protects against OOM floods."* No: an `OutOfMemoryError` is an `Error`, which halts rather than quarantines.
- *"The lock file left on disk means the journal is still open."* The OS lock, not the file's existence, means "open"; the empty file stays by design.
- *"A green test suite on Windows covers the lock on Linux."* POSIX lock semantics differ fundamentally; that difference is the reason for the redesign.
- *"CRLF tolerance is a bug fix."* It is a format decision: the journal format says LF, and changing what the decoder accepts affects replay of old journals.

## Questions to test yourself

1. Why could an `OutOfMemoryError` caused by a flood make the exchange unable to restart, while an engine `IllegalStateException` on one order does not stop it?
<details><summary>Answer</summary>The ISE is a `RuntimeException`: live, it is quarantined (SKIP + rebuild) and the exchange continues. The OOM is an `Error`: the writer halts and the process exits 2. On restart, replay rebuilds the same id map and book from the journal, which can need the same memory and fail again.</details>

2. You open `exchange.journal` in Notepad, save it, and restart. What happens and why?
<details><summary>Answer</summary>If the editor converted line endings to CRLF, startup fails with "Bad journal header ... For input string: "1\r"": the decoder splits on `,` and keeps the `\r` in the last field (observed live). The data is intact but needs converting back to LF.</details>

3. Which of these are deliberate policies rather than bugs: self-trades allowed; `QUIT` dropping replies; a fully fillable LIMIT rejected at the cap; `--quarantine` running next to a live exchange?
<details><summary>Answer</summary>Self-trades and the cap rule are documented policies (Spec §8). `QUIT` dropping replies and quarantine next to a live exchange were defects, fixed as D63 and D65.</details>

4. What evidence shows the D63 regression test really guards the bug?
<details><summary>Answer</summary>It was run against the original `ClientSession` restored from git and failed (1 of 200 `ORDER` lines before `BYE`), then passed with the fix (Spec §10).</details>

5. Why was the first D65 lock implementation replaced, though its tests passed?
<details><summary>Answer</summary>It locked a byte of the journal file itself. On Linux/macOS Java's locks are POSIX locks, released when the process closes any handle to that file, and the journal code opens and closes such handles (`read()`, header check, torn-tail cut). The tests passed because they ran on Windows, where locks are per handle.</details>

6. Pick one Low item and say what minimal change would fix it.
<details><summary>Answer</summary>E.g. L1: after `ExchangeService.start`, have `Main` print `replayedOnOpen()`, `truncatedBytesOnOpen()` and `quarantined()` (they would need exposing through `ExchangeService`, since `Main` doesn't hold the `Exchange`). Or L3: make `Exchange.quarantine` refuse a sequence number that already has a SKIP.</details>
