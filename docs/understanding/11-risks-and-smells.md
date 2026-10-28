# 11 — Risks and smells

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** an honest review after two rounds of fixes on 2026-10-27: what was found and fixed (with decisions and tests), and the risks and smells that remain, ranked | **Prerequisites:** 01, 05, 08 | **Related:** 09a, 09b, 10, 12, 15
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

This is the "what could hurt you" guide. It went through two rounds on 2026-10-27:

1. **Survey fixes.** Writing these guides found four real defects (a flaky test, lost replies on QUIT, a journal open that damaged foreign files, no single-writer lock). They were fixed as D63–D65 (§1.1).
2. **Risk fixes.** The first version of this guide listed 3 High, 10 Medium and 6 Low risks plus smells. Four of them were security findings the user had **deferred** earlier that day (unauthenticated LAN bind, a memory flood, no timeouts, no fairness). The user then asked for **every risk to be fixed, deferred ones included**. They were fixed as D66–D77 and verified on Windows and Linux (§1.2).

What is left (§2) is smaller and different in kind. Some are risks **introduced by the fixes** (more code, more moving parts, new operational duties). Some are deliberate limits of the new features. Some are paths that have still never been exercised.

**Severity scale** (for a single-machine learning system, not a production venue):
- **High**: could lose data, wedge the system, or let an outsider act as a participant under realistic use.
- **Medium**: wrong or surprising behaviour in an edge case, an operational trap, or a gap that will bite as the project grows.
- **Low**: inefficiency, misleading output, test fragility.
- **Smell**: not a bug; makes the code harder to read or change.

Each remaining entry gives **location · evidence · impact · label · direction**.

---

## 1. Resolved on 2026-10-27

### 1.1 Round one: survey fixes (D63–D65)

| # | Finding | Fix | Regression test(s) |
|---|---|---|---|
| 1 | **Flaky test**: the writer's stats were read before the writer updated them | Tests read stats after `stop()` | five `ExchangeServiceTest` assertions |
| 2 | **QUIT/EOF lost replies** of orders that executed (199 of 200 against the old code) | D63, later reworked as D76 | `TcpGatewayTest.Session.quitAnswersEveryEarlierRequestBeforeBye`, `endOfStreamStillGetsTheReplies` |
| 3 | **`open` damaged foreign files** (cut a "torn tail" before checking the header; emptied a one-line file) | D64 | `CommandJournalTest.Header.fileThatIsNotAJournalIsRefusedUnchanged` and two more |
| 4 | **No single-writer lock**: `--quarantine` could append next to a running exchange | D65 (first a byte lock on the journal; reworked the same day to `<journal>.lock` because POSIX locks drop when any handle closes) | `CommandJournalTest.Exclusive.*` incl. a child JVM; now passing on Windows **and Linux** |

All seven regression tests of round one failed against the original code and passed after the fix [Verified — Spec §10].

### 1.2 Round two: every risk in the first review (D66–D77)

| Was | Risk | Fixed by | Evidence |
|---|---|---|---|
| H1 (deferred) | Unauthenticated gateway on every interface, no TLS | **D74**: loopback bind by default, `LOGIN` with a secret (salted PBKDF2, constant time, no oracle, one attempt per connection), optional TLS 1.3/1.2 | `CredentialsTest`; `TcpGatewayTest.Security.*` incl. `tlsRoundTrip`; live: `ERROR login failed` |
| H2 (deferred) | A memory flood that replay reproduces | **D68** global cap (`BOOK_FULL`), **D74** no free names, **D75** rate and in-flight limits, **D71** id map scoped per journal | `MatchingEngineTest.Caps`; `TcpGatewayTest.Limits.*` |
| H3 | History only grows; no snapshots; rules changes cannot be crossed | **D71** snapshots + offline compaction; empty journals adopt new rules | `ExchangeTest.Compaction.*` (8 tests); live compaction and restart from the snapshot |
| M1 (deferred) | No socket timeouts | **D75** login 10 s, idle 300 s | `TcpGatewayTest.Limits.aConnectionThatNeverLogsInIsDisconnected`, `anIdleSessionIsDisconnected` |
| M2 (deferred) | No fairness: one client fills the queue | **D75** per-session in-flight cap and token bucket | `TcpGatewayTest.Limits.requestsBeyondTheInFlightLimitAreRefused`, `requestsBeyondTheRateAreRefusedWithoutReachingTheJournal` |
| M3 | Durable ⇒ visible gap with no client recovery | **D73** numbered private stream + `RESEND` (the gap itself remains by design, §2) | `TcpGatewayTest.Resend.*` |
| M4 | D65 lock never run on Linux/macOS | Full suite + live check **on Linux** (Docker) | 446 green on Linux; live `--quarantine` refused |
| M5 | `Rules.VERSION` misses code behaviour changes | **D70** output digest + CHECK records | `ReplayTest.Check.*`, `ExchangeTest.Checkpoints.aChangedHistoryIsCaughtAtTheNextCheck` |
| M6 | No per-record checksum; directory not fsynced | **D69** CRC-32C per record, directory fsync | `JournalCodecTest.Corrupt.aChangedCharacterFailsTheChecksum` |
| M7 | A CRLF conversion bricks startup with a puzzling error | **D69** a specific error message + `.gitattributes` | `JournalCodecTest.Corrupt.aCarriageReturnIsReportedAsALineEndingConversion` |
| M8 | Disconnect leaves orders resting; missed fills lost | **D72** opt-in cancel-on-disconnect, **D73** RESEND | `TcpGatewayTest.CancelOnDisconnect.*` |
| M9 | Exceptions in callbacks vanish | **D77** logged | `ExchangeServiceTest.Robustness.aThrowingCallbackIsLoggedAndTheExchangeCarriesOn` |
| M10 | Policy edges: self-trades allowed; cap on fully fillable orders; first market order sweeps; D63 fails in shutdown | **D67** self-trade prevention, **D68** caps only for resting, **D66** collar before the first trade, **D76** in-flight close | `MatchingEngineTest.SelfTrade`, `Caps`, `Market`; `TcpGatewayTest.Session.quitDuringShutdownStillGetsTheRepliesOfAcceptedRequests` |
| L1 | Startup says nothing about recovery | **D77** recovery summary | live: `Recovered through sequence 4 from snapshot 4: ...` |
| L2 | Misleading operator messages | **D77** `ReplayException` kinds + `Main.hint`; compaction advice | `MainTest` |
| L3 | `--quarantine` accepted an already-skipped command | **D77** refused | `ExchangeTest.Operations.quarantineRefusesACommandAlreadyQuarantined` |
| L4 | `linesUntil` dropped equal lines | consumes exactly what it returns | `ExchangeClientTest.linesUntilConsumesExactlyTheLinesItReturns` |
| L5 | Unbounded spin-wait in a test | deadline added | `src/test/java/exchangeserver/core/ExchangeServiceTest.java:352` |
| L6 | `stop()` from the writer; acceptor without alarm; silent clients during replay; exit code after a signal | **D77** throw; exponential backoff + ERROR after 10; "starting" answer; hook ends with 0 | `ExchangeServiceTest.Robustness.stopFromTheWriterThreadIsRefused`; `TcpGatewayTest.Session.beforeTheExchangeIsAttachedSessionsAreToldItIsStarting`; Linux: exit 0 after SIGINT and SIGTERM |
| Smells | `STOP` field named like a constant; unused `getFile()`; an unused `start` overload | renamed `stopMarker`; both deleted | — |

[Verified — Spec D66–D77, §11; tests named above exist and pass.] Two problems were found only by **running** things during round two, which is worth remembering: a test journal written with `Files.write` was refused because it had CRLF line endings (the new check working as designed), and `--add-user` rejected a secret piped from PowerShell because PowerShell prepends a byte-order mark, which `Main` now drops [Verified — `src/main/java/exchangeserver/Main.java:224-230`].

---

## 2. What remains

There are **no High items** for the intended use (local or LAN, a handful of known participants, TLS when exposed).

### 2.1 Medium

#### R1. Durable ⇒ visible still does not hold (by design)
- **Location:** the write-ahead order in `Exchange.process` (journal, then process); `ExchangeService.halt` [Verified — `src/main/java/exchangeserver/core/Exchange.java:198-252`].
- **Evidence:** a crash or halt after a batch's fsync and before its replies leaves journalled commands whose callers heard `HALTED` or nothing; they take effect on restart [Verified — Spec §4, §8].
- **Impact:** a client that does not reconcile can believe an order failed while it rests. Remedies now exist: resend with the same client order id within the journal (D49/D71), or log in and `RESEND` from the last oseq seen (D73). Both are client duties.
- **Direction:** document the reconnect procedure for client authors (`RESEND last+1`), since nothing forces it.

#### R2. Replay correctness still relies on discipline at upgrade time
- **Location:** `CHECK` every `checkInterval` commands; `--compact` must be run by the build that wrote the journal [Verified — `src/main/java/exchangeserver/core/Exchange.java:200-203`; `src/main/java/exchangeserver/journal/CommandJournal.java:252-259`].
- **Evidence:** a behaviour change is detected only at the next CHECK after it, and only if an output differs; a journal with records written under other rules is refused until compacted by the old build; v2/rules-1 journals from before the fixes cannot be opened at all [Verified — Spec §8].
- **Impact:** an operator who upgrades without compacting first gets a startup refusal (safe, but an outage); a subtle change that alters only internal state (not outputs) until much later passes the earlier CHECKs [Inferred].
- **Direction:** a release checklist (compact, then upgrade); optionally a CHECK at clean shutdown so the last stretch is covered too.

#### R3. Security is basic
- **Location:** `Credentials` (participant + secret only), loaded once in `Main.serve`; per-session limits; TLS optional [Verified — `src/main/java/exchangeserver/Main.java:124`; Spec §8].
- **Evidence and impact:**
  - No accounts, roles or revocation without a restart (the users file is read once).
  - Rate and in-flight limits are **per session**; a participant can open many sessions (up to the 256 cap), multiplying its budget.
  - PBKDF2 (210,000 iterations) runs on each session's reader thread: a burst of logins costs CPU (bounded by the session cap and one attempt per connection) [Inferred — cost not measured].
  - The Java client does **not verify the server's host name** (plain `SSLSocketFactory.createSocket`), so trusting a broad CA set would accept any certificate from it [Verified — `src/main/java/exchangeserver/client/ExchangeClient.java:45-46`; JSSE behaviour Inferred].
  - `--bind` to a network address without TLS only **warns**; secrets then cross the network in clear [Verified — `src/main/java/exchangeserver/Main.java:127-129`].
  - Failed logins are not logged, so guessing is invisible to the operator [Verified].
- **Direction:** refuse a non-loopback bind without TLS (or require a flag); enable endpoint identification on the client (`SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")`); log failed logins; per-participant limits.

#### R4. Paths and platforms never exercised
- **Evidence:** **Windows Ctrl+C** was never sent (the exit code was verified with SIGINT/SIGTERM on Linux only); **macOS** never run; **network file systems** never tried for the journal lock; **TLS with a real CA** never tried; slow-consumer disconnect and "too many sessions" still have no test; the gateway, client and `Main` are not mutation-tested [Verified — guide 10 §8].
- **Impact:** the documented behaviour of those paths is reasoned, not demonstrated.
- **Direction:** a CI matrix (Windows, Linux, macOS); a child-JVM test for signal exit on Linux; a small-capacity gateway test for the slow consumer.

#### R5. Performance after the fixes is unknown
- **Evidence:** the JMH numbers in Spec §5 predate the fixes and were not re-run. Each command now also builds a text encoding for the digest and allocates private-event records; FOK and at-cap orders walk resting orders with one copy per visited order; each login runs PBKDF2 [Verified — `src/main/java/exchangeserver/core/OutputDigest.java:48-83`; `src/main/java/exchangeserver/book/OrderBook.java:149-158`; benchmarks not re-run].
- **Impact:** probably still dominated by fsync (~1.5 ms vs ~1 µs per command), but unmeasured [Inferred].
- **Direction:** re-run `MatchingBenchmark` and `BatchingBenchmark`; if the digest shows up, hash fields directly instead of building strings.

#### R6. Recovery features have limits clients must know
- **Evidence:** RESEND reaches back 1,000 events per participant and never past the last compaction; market data has no resend; cancel-on-disconnect runs on a clean shutdown (sessions are closed before the stop) but **not on a crash**; COD triggers only when the participant's *last* session closes [Verified — Spec D72, D73, §8].
- **Impact:** a client offline for long, or across a compaction, can miss private events (it is told `RESEND_GAP`); after a crash, COD participants' orders are still resting on restart.
- **Direction:** document; consider a COD sweep at startup for participants with COD sessions at crash time (would need the COD flag journalled).

#### R7. A much larger, more interlocking codebase
- **Evidence:** production code grew from 3,801 to 5,710 lines (about 50%): a ledger, digest, output log, snapshot format, journal lock and durability helpers, credentials, TLS, session limits, six new decisions that depend on each other (guide 09b) [Verified — line counts of `src/main` at commit d930e53 and after the fixes].
- **Impact:** more to learn, and ordering invariants that are easy to break: listener-before-future (D76), Attach joining the stream on the writer, the ledger rebuilt as a unit, oseq derived only from outputs. A change that reorders `Exchange.process` would break D76 silently except for the tests that pin it [Inferred].
- **Direction:** keep the pinning tests; guides 05 and 09b describe the orderings.

### 2.2 Low

| # | Risk | Evidence | Direction |
|---|---|---|---|
| R8 | Stray files accumulate | Snapshots, archives and the lock file are never deleted automatically [Verified — Spec D71] | an operator clean-up step |
| R9 | CANCEL_ALL walks the whole book | O(book) per mass cancel; no per-participant index [Verified — `src/main/java/exchangeserver/engine/MatchingEngine.java:140-149`] | fine at this scale |
| R10 | Observability is thin | No metrics; connections, logins and refusals are not logged [Verified — guide 08 §2] | metrics are a Vision candidate |
| R11 | Thread-per-connection | 512 platform threads at the 256-session cap [Verified — Spec §8] | NIO or virtual threads (Vision candidate) |
| R12 | Compaction is offline | An end-of-day stop is required [Verified — Spec §8] | online rotation (Vision candidate) |
| R13 | A self-trade-blocked participant cannot reach better prices behind its own order | By design (stop, do not skip) [Verified — Spec D67] | document for client authors |
| R14 | The idle timeout disconnects listen-only subscribers | 300 s without a line; clients must PING [Inferred from D75] | document; or count market data as activity |

---

## 3. Smells still present (each justified)

| Smell | Location | Why it stays |
|---|---|---|
| `instanceof` chains over sealed types | `Exchange.process`, `Ledger.apply`, `Replay.feed`, `JournalCodec.encode`, `OutputLog.record`, `ClientSession.handle` | Pattern-matching `switch` is a preview feature in Java 17 (final in 21); the cost is no compiler exhaustiveness check for these chains [Verified/Inferred] |
| Benchmark-only linger mode in production code | `src/main/java/exchangeserver/core/ExchangeService.java:295`, `326` | Kept so `BatchingBenchmark` can compare (D52) |
| Convenience API on `Exchange` | `src/main/java/exchangeserver/core/Exchange.java:289` | Used by tests, tools and the benchmark baseline |
| Captured arrays for lambda state | `long[] fed` (`src/main/java/exchangeserver/core/Replay.java:74`), `long[] last`, `boolean[] exists` (`src/main/java/exchangeserver/core/Exchange.java:143-145`), `boolean[] wasLast` (`src/main/java/exchangeserver/gateway/TcpGateway.java:247`) | A common Java idiom; small classes would read better |
| Unchecked cast to complete a future | `src/main/java/exchangeserver/core/Pending.java:27-30` | Type safety rests on `process` pairing each request with its result type |
| Sentinels by identity | `new String("<close>")` in `ClientSession`; `"\u0000EOF"` in `ExchangeClient` | Deliberate and correct, surprising to readers |
| Defensive double checks | `remaining > 0` in `OrderBook.add` and `PriceLevel.addOrder`, etc. | Each layer protects its own invariant; the reason several PIT boundary mutants are equivalent |
| Test-only seams in production classes | `MatchingEngine.Limits`, `TcpGateway.drainingSessions()`, `ClientSession.draining()` | Package-private; the alternative (reflection or huge test data) is worse |
| Comments coupled to decision numbers and external documents | D-numbers everywhere; "threads guide" (Spec §1.6) | Useful as a trail; needs the Spec at hand |

### Traits typical of AI-written code, as observed here

The user asked for these to be called out. Each is a pattern seen in this codebase [Verified examples]:

1. **Very dense, decision-referencing comments.** Accurate and explanatory, but they assume the Spec is open.
2. **Confident status claims that did not hold until checked.** "326 green" hid a flake; the first D65 looked right and was wrong for POSIX; a fix that "didn't work" was a stale jar. **Run things.**
3. **Defensiveness inside, gaps at the edges** (round one): strong internal invariants next to no authentication or timeouts. Round two closed the edge gaps, at the cost of a much larger surface (R7).
4. **Fixes that build on fixes.** D76 replaced D63 the same day; D65 was reworked the same day. The decision log records each step, which is honest, but a reader must follow chains (09a "Revised by" lines).

---

## 4. Things that look like risks but are fine

- **No locks in the book, engine or ledger.** Correct by the single-writer design (guide 08 §5).
- **Unbounded request queue.** Capacity is enforced under the hand-in lock, and per session by D75; unbounded only so `stop()` can always enqueue its marker (D56).
- **`HashMap`s in the engine, book and output log.** Only looked up; walks go through the levels, and the snapshot sorts participants.
- **Rate limits using the wall clock.** They run on reader threads and refuse before the queue; nothing about a refused request is replayed.
- **A snapshot "replacing" history.** It is a checkpoint of derived state; the journal archive keeps every input.
- **Every session of a participant receiving all its replies.** Deliberate (D73): one numbered stream per participant.

---

## 5. Top 5 remaining risks

1. **R1 — Durable ⇒ visible gap (by design)**: after a crash or halt, journalled orders whose clients heard nothing take effect on restart; clients must reconcile with RESEND or a same-id resend.
2. **R3 — Basic security**: no accounts or revocation without a restart, per-session (not per-participant) limits, no host-name verification in the client, and a network bind without TLS is only warned about.
3. **R2 — Upgrade discipline**: compaction must be run by the old build before a rules or behaviour change; divergence is caught only at CHECK intervals; pre-fix journals cannot be opened.
4. **R4 — Unexercised paths**: Windows Ctrl+C, macOS, network file systems, TLS with a real CA, slow-consumer and too-many-sessions handling, and the unmutated gateway.
5. **R5 — Unmeasured performance after the fixes**: benchmarks not re-run while per-command allocation grew (digest encoding, event records) and logins now cost PBKDF2.

---

## Key takeaways

- Every risk from the first review is fixed, including the four security findings the user first deferred; each has a decision (D63–D77) and tests, and the suite (446 tests) passes on Windows and Linux.
- What remains is mostly **by design** (the durable ⇒ visible gap), **operational duty** (compact before upgrading, client reconciliation), **basic security** (no accounts, per-session limits) and **unexercised paths** (Windows Ctrl+C, macOS).
- The fixes **added risk of their own**: a larger, interlocking codebase whose ordering rules (D76, Attach, ledger rebuild) must be preserved.
- Two Windows-specific bugs were found only by running things (CRLF in a test journal, a PowerShell byte-order mark): verification by execution keeps paying off.
- No High risks remain for the intended local/LAN use.

## Common confusions

- *"Fixed means gone."* The durable ⇒ visible gap is not gone; D73 gives clients a way to recover from it.
- *"TLS is on."* Only with `--tls-keystore`; the default protection is the loopback bind plus secrets.
- *"CHECK proves the build is unchanged."* It proves the outputs matched up to the last CHECK.
- *"Deferred findings were ignored."* They were deferred, then fixed the same day at the user's request (D68, D74, D75).
- *"The Linux run proves macOS."* POSIX locks behave alike there in principle, but macOS was never run.

## Questions to test yourself

1. Which first-review risks were security findings the user deferred, and which decisions fixed them?
<details><summary>Answer</summary>H1 (LAN bind, no auth) → D74; H2 (memory flood) → D68, D74, D75 (and D71 for the id map); M1 (no timeouts) → D75; M2 (no fairness) → D75.</details>

2. A client crashes after sending NEW and reconnects. How does it learn whether the order exists, now?
<details><summary>Answer</summary>Log in: WELCOME reports the latest oseq; `RESEND <last oseq it saw + 1>` returns the ORDER line (and any fills) it missed. Alternatively resend the same NEW with the same client order id: a DUPLICATE reject carries the original order id (within the same journal).</details>

3. Why is "compact with the old build" an operational risk and not just a step?
<details><summary>Answer</summary>If an operator upgrades first, the new build refuses the journal (records under other rules) or, for an unannounced behaviour change, stops at the next CHECK with DIVERGED. Both are safe refusals but mean an outage until the old build compacts. Nothing enforces the order except the refusal itself.</details>

4. Give one risk the fixes introduced and one way to guard it.
<details><summary>Answer</summary>E.g. the ordering invariant "listener before future" (D76): reordering `Exchange.process` would break close-after-replies; it is guarded by `ExchangeServiceTest.HandIn.listenerRunsOnTheWriterBeforeTheCallback` and the gateway shutdown test. Or the per-command allocation for the digest: guard by re-running the benchmarks.</details>

5. Why is the absence of host-name verification in `ExchangeClient` a risk even though TLS is used?
<details><summary>Answer</summary>Without endpoint identification the client accepts any certificate its truststore trusts, not just one for the host it meant to reach. With a private truststore holding only the exchange's certificate that is fine; with the JDK's default CA set, any CA-issued certificate would be accepted.</details>
