# 15 — Open Questions

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** what was open and how it was resolved, what is still open or unverified, and the questions you should be able to answer after studying | **Prerequisites:** ideally all guides; at least 01, 05, 09, 11 | **Related:** 09, 11, 12, 14
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

The first version of this guide listed 24 open questions. On 2026-10-27 the user asked for every risk to be fixed, including the four security findings deferred earlier that day, and chose four policies: credentials + localhost + optional TLS; self-trade prevention by cancelling the incoming remainder; RESEND plus opt-in cancel-on-disconnect; work on a branch with commits and a Linux check in Docker. The result is Spec D66–D77. So this guide now has four parts:

1. **Resolved** — each former question with its answer and where it lives.
2. **Questions for the user** that remain (policy and scope).
3. **Not verified** by the sessions that wrote the code and these guides.
4. **Questions you should be able to answer after studying** (no answers).

---

## 1. Resolved (2026-10-27)

| # | Former question | Resolution |
|---|---|---|
| 1 | Gateway on every interface, unauthenticated LOGIN (deferred security finding 1) | **Fixed at the user's request** (D74): bind 127.0.0.1 by default (`--bind` to widen), `LOGIN <p> <secret>` against salted PBKDF2-HMAC-SHA256 hashes (`--add-user`), one attempt per connection, optional TLS 1.3/1.2 (`--tls-keystore`). `src/main/java/exchangeserver/gateway/Credentials.java`, `src/main/java/exchangeserver/gateway/Tls.java`, `src/main/java/exchangeserver/gateway/TcpGateway.java:52-98`. Authentication and TLS left the Vision's non-goals. **[Verified]** |
| 2 | Memory-exhaustion flood (finding 2) | **Fixed** (D68, D71, D74, D75): a global cap `MAX_RESTING_ORDERS` (1,000,000, `BOOK_FULL`), only authenticated names, a per-session rate limit, and compaction scoping the client-order-id map to a journal. **[Verified]** |
| 3 | No timeouts (finding 3) | **Fixed** (D75): login timeout 10 s, idle timeout 300 s. `src/main/java/exchangeserver/gateway/ClientSession.java:152-183`. **[Verified]** |
| 4 | No fairness (finding 4) | **Fixed** (D75): per-session in-flight cap (`SESSION_BUSY`) and token-bucket rate limit (`RATE_LIMITED`). `ClientSession.java:279-291`. **[Verified]** |
| 5 | Cancel-on-disconnect | **Opt-in** (D72): `LOGIN … COD`; the last COD session closing submits a journalled `CANCEL_ALL`. `TcpGateway.java:239-263`. **[Verified]** |
| 6 | Self-trade policy | **Cancel the incoming remainder** (D67), `SELF_TRADE_PREVENTED`. `src/main/java/exchangeserver/engine/MatchingEngine.java:224-227`. **[Verified]** |
| 7 | Open-order cap on a fully fillable LIMIT | **Fixed** (D68): caps apply only to an order that would rest. `MatchingEngine.java:193-200`. **[Verified]** |
| 8 | Client-order-id lifetime | **Per journal** (D71 revises D49): after compaction only resting orders keep their ids. **[Verified]** |
| 9 | Snapshots and rotation | **Offline compaction** (D71): `--compact` writes `<journal>.snapshot-<seq>`, archives the journal, leaves an empty one with that base. `src/main/java/exchangeserver/core/Exchange.java:174-196`. **[Verified]** |
| 10 | Startup reporting | **Summary line** (D77): `Recovered through sequence N [from snapshot B]: replayed R commands, cut T torn bytes …`. `src/main/java/exchangeserver/Main.java:154-157`. **[Verified]** (live) |
| 11 | Writer stats lag replies | Documented and kept: tests read them after `stop()` (Spec §10 #1). Nobody needs them exact at reply time yet. |
| 12 | CRLF tolerance | **Detect, don't tolerate** (D69): a record ending in `\r` is reported as a line-ending conversion; `.gitattributes` marks journals `-text`. **[Verified]** |
| 13 | Double SKIP | **Refused** (D77): "Command N is already quarantined". `Exchange.java:141-171`. **[Verified]** |
| 14 | Generic startup hint | **Tailored** (D77): poison-pill advice only for `ReplayException(ENGINE_FAILURE)`, compaction advice for `DIVERGED`, otherwise "The journal was not changed." `Main.java:187-198`. **[Verified]** (`MainTest`) |
| 16 | D63's snapshot barrier | **Replaced** (D76): an in-flight count per session; the listener runs before the future, so the last callback closes after the lines went out. This also holds during shutdown, where the barrier read was refused. **[Verified]** (`TcpGatewayTest.Session.quitDuringShutdownStillGetsTheRepliesOfAcceptedRequests`) |
| 18 | Journal lock on Linux/macOS | **Verified in Docker** (`eclipse-temurin:17`, Linux kernel under WSL2): the full suite passed, including the child-JVM test, and `--quarantine` against a running server was refused. Spec §11. **[Verified]** |
| 19 | Directory fsync | **Done** (D69): `Durable.fsyncDirectory` after creating or renaming files (ignored on Windows, where a directory cannot be opened). `src/main/java/exchangeserver/journal/Durable.java:50-64`. Not observable by any test. |
| 21 | Clean Ctrl+C shutdown | The hook path ran on Linux (SIGINT and SIGTERM) — see item 24. Windows Ctrl+C itself was not exercised (item N7). |
| 22 | Mutation results after the fixes | Recorded in Spec §7 by the session that made them. |
| 24 | Exit code after Ctrl+C | **0, by construction and verified**: the shutdown hook ends the process with `Runtime.halt(status)` (0, or 2 if halted, D77). In Docker with job control on, SIGINT and SIGTERM both gave exit status 0. Note: a non-interactive shell starts background jobs with SIGINT ignored, so test with `set -m`. `Main.java:160-177`. **[Verified]** |

Item 15 (original-project documents cited by comments) and 17 (collaboration mode) remain below; 20 and 23 (benchmarks, linger on Linux) remain unverified.

---

## 2. Questions for the user

N1. **Online or automatic compaction.** Compaction runs only with the exchange stopped (D71). *Should it run on a schedule, at a size threshold, or online (the writer swapping journals between batches)?* Online needs a design for the swap and for clients' RESEND across it.

N2. **Retention size.** RESEND keeps 1,000 events per participant (`--resend-window`) and never reaches past a compaction (D73). *Is that enough for your clients' reconnect gaps? Should retention be per time instead of per count?*

N3. **Market-data resend.** Private events can be resent; TRADE/LEVEL cannot (re-SUBSCRIBE gives a fresh snapshot). *Wanted?*

N4. **Per-participant vs per-session limits.** Rate and in-flight limits are per session (D75); a participant can open several sessions (bounded by 256 total). *Limit per participant instead, or as well?*

N5. **Reloading users without restart.** The users file is read at startup only. *Add a reload (an admin command, or watching the file)? And what happens to sessions of a removed participant?*

N6. **PBKDF2 cost vs login latency.** 210,000 iterations make each LOGIN cost tens to hundreds of milliseconds on the reader thread **[Inferred]** — fine for a few logins, but a login storm costs CPU. *Lower, keep, or add a per-address login rate limit?*

N7. **Secrets rotation and policy.** `--add-user` replaces an entry; there is no expiry, no lockout after failures beyond "one attempt per connection". *Needed for a learning project?*

N8. **Self-trade policy variants.** "Cancel incoming" was chosen; "cancel resting" and "cancel both" are not available. *Ever make it a per-order or per-participant choice?*

N9. **Old journals.** Format v2 / rules v1 journals from before D69 cannot be opened, and there is no migration tool (they predate snapshots). *Write one, or treat them as disposable?*

N10. **Original-project documents** (former 15). Comments cite "threads guide" and "Spec v8" in the original project; Spec §1.6 names them. *Copy them in?*

N11. **Collaboration mode** (former 17). D63–D77 were LLM-written on the user's instruction. *Which mode for the next piece of work?* (`CLAUDE.md`, Vision §3.)

N12. **Digest strength.** The output digest is FNV-1a 64 (D70): good at catching accidental divergence, useless against deliberate tampering, and only checked every 10,000 commands. *Is a cryptographic hash or a shorter interval worth the cost?*

---

## 3. Not verified

V1. **macOS.** Nothing has run on macOS; Linux ran in Docker (Spec §11).
V2. **Windows Ctrl+C.** The exit-code fix was verified with SIGINT/SIGTERM on Linux; a console Ctrl+C on Windows was not exercised from the tooling **[Inferred]** to behave the same, since the same hook runs.
V3. **Benchmarks** (former 20, 23). Spec §5 numbers were not re-run after D66–D77. The new per-command work (digest update, output-log entry, private-event lines) adds allocation the benchmarks do not yet measure **[Inferred]**.
V4. **Network file systems.** Lock semantics there may differ (D65) **[Inferred]**.
V5. **Login cost.** The PBKDF2 time per login was not measured.
V6. **TLS performance and client interop.** Tested with the JDK's own client in `TcpGatewayTest.Security.tlsRoundTrip`; no other TLS client was tried.

---

## 4. Questions you should be able to answer after studying

No answers here; each points to where the answer lives.

**Architecture and flow**
1. Trace a crossing NEW from the socket to the maker's FILL line, naming every thread and class. *(05 flow 1)*
2. Why does sequence order equal processing order without a lock around the `Sequencer`? *(07 exchange-service; D55)*
3. Why does the listener run before the request's future completes, and what would break if it did not? *(07 exchange-and-replay; D76)*
4. Why do a participant's replies go to all its sessions? *(07 gateway; D73)*

**Correctness**
5. Where exactly does self-trade prevention stop matching, and why does the FOK pre-check have to stop at the same place? *(07 engine; D67)*
6. Why do the caps ignore an order that fully trades on arrival? *(07 engine; D68)*
7. How is the market collar anchored before the first trade? *(07 engine; D66)*

**Durability and recovery**
8. What does a CHECK record prove, and what can it miss? *(07 ledger-and-snapshots; D70)*
9. Walk through a crash at each step of compaction: which journal and snapshot does the next startup use? *(07 ledger-and-snapshots; D71)*
10. How do you move a journal across a `Rules.VERSION` change? *(12; D71)*
11. Why does a CRC catch what the parser cannot? *(07 journal; D69)*
12. Why is oseq rebuilt identically by replay, and what does a snapshot keep of it? *(07 ledger-and-snapshots; D73)*

**Concurrency and the gateway**
13. How does `ClientSession` decide when a QUITting session may close, and why is it safe whichever thread gets there last? *(07 gateway; D76)*
14. Why is the rate limit outside the engine? *(08; D75)*
15. Why must `JournalLock` lock a separate file? *(07 journal; D65)*

**Design judgement**
16. Which changes need `Rules.VERSION`, which `FORMAT_VERSION`, and which are caught only by a CHECK? *(12)*
17. Name three guarantees no test can verify, and how you would check them in production. *(11; 14 (e))*

---

## Key takeaways

- Every risk from the review, including the deferred security findings, was addressed on 2026-10-27 (D66–D77), with the four policy choices made by the user.
- What remains open is mostly *scope*: online compaction, retention, market-data resend, limits per participant, users reload, digest strength.
- Linux is verified (Docker); macOS, Windows Ctrl+C and the benchmarks are not.
- If you can answer section 4 without the guides, you understand the project the way its author does.

## Common confusions

- **"Deferred means still open."** The four security findings were deferred, then fixed at the user's later request.
- **"Compaction runs by itself."** It is an offline operator tool.
- **"RESEND recovers everything."** It reaches back 1,000 events per participant and never past the last compaction; market data is not resent.
- **"Exit code 0 after Ctrl+C is just documentation."** It is enforced by the shutdown hook and was observed on Linux.

## Questions to test yourself

1. Which resolved item needed a new journalled command type, and why could it not be done in the gateway alone?
<details><summary>Answer</summary>Cancel-on-disconnect: a disconnect changes the book, so it must be a stamped, journalled command (<code>CANCEL_ALL</code>) processed by the writer thread; the gateway may not touch the book and replay must see the change.</details>

2. Why does the Linux run matter for the journal lock but not much for the matching engine?
<details><summary>Answer</summary>File-lock semantics differ by OS (POSIX locks per process, dropped on any close; Windows per handle), while the engine is pure deterministic Java that behaves the same everywhere.</details>

3. A client misses fills while disconnected for a week with heavy trading. What can it recover?
<details><summary>Answer</summary>Only the last 1,000 of its private events (by default), and nothing from before the last compaction: RESEND answers with <code>RESEND_GAP first=…</code> for the rest.</details>
