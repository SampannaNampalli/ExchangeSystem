# 14 — Exercises

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** active learning from easy to hard: self-check questions, predict-then-verify runs, debugger traces, hands-on changes and break-it experiments | **Prerequisites:** 01, 05 (do the rest as each exercise's tags say) | **Related:** all guides, especially 05, 07, 10, 11, 12
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

Five kinds of exercise, easiest first. Each one is tagged with the guides it reinforces, e.g. `[05, 07-engine]`. Answers are hidden in `<details>` blocks: commit to an answer before you open one.

**Setup used throughout** (Windows; on macOS/Linux use `./mvnw` and `/`):

```
mvnw.cmd -q package -DskipTests
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal study.journal
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000
```

`ClientMain` sends what you type and prints every server line with `< ` in front **[Verified]** (`src/main/java/exchangeserver/client/ClientMain.java:20-50`). Open two client windows for two participants. Delete `study.journal` to start fresh (journals are git-ignored, `.gitignore`). Stop the server with Ctrl+C (exit code 0).

For code changes: work on a throwaway branch (`git switch -c study/<name>`), then `git switch main` and `git branch -D study/<name>` when done. After swapping files around, build with `mvnw.cmd clean …` (incremental compilation goes by timestamp, `CLAUDE.md`). Never run two Maven builds at once.

---

## (a) Self-check questions

1. `[01, 05]` In what order does the writer thread do these for a batch: process in the engine, fsync, stamp, reply?
<details><summary>Answer</summary>Stamp every request (Sequencer) → append all with one write and one fsync → for each: engine → complete the future/callback (reply) → OutputListener. <code>src/main/java/exchangeserver/core/Exchange.java:118-166</code>.</details>

2. `[07-domain-and-commands]` Which single class reads the clock? What happens if the system clock steps back one second?
<details><summary>Answer</summary><code>command.Sequencer</code> (<code>Sequencer.java:74-81</code>). If the clock goes back, it reuses the previous timestamp, so journal timestamps never decrease (D33).</details>

3. `[07-book]` Why does a `PriceLevel` need sentinel nodes?
<details><summary>Answer</summary>With a dummy head and tail, every real node always has a non-null <code>prev</code> and <code>next</code>, so append and unlink are four pointer assignments with no "is this the first/last node?" branches (<code>PriceLevel.java:11-15, 115-134</code>).</details>

4. `[07-engine, 06]` A BUY LIMIT 4 @ 101 hits an ask of 10 @ 100. At what price is the trade, and who gets which lines?
<details><summary>Answer</summary>At 100, the resting price (<code>Trade.java:10</code>). The buyer gets <code>FILL … side=BUY price=100 qty=4 leaves=0 liquidity=TAKER</code> then <code>ORDER … status=FILLED</code>; the seller gets <code>FILL … leaves=6 liquidity=MAKER</code>; subscribers get <code>TRADE</code> and <code>LEVEL … qty=6</code>.</details>

5. `[07-engine, 09]` Why is a cancel of someone else's order answered `UNKNOWN_ORDER` and not `NOT_OWNER`?
<details><summary>Answer</summary>D48: a distinct status would let any participant probe which order ids are live; the caller's next action is the same either way.</details>

6. `[07-journal]` What are the steps of `CommandJournal.open`, in order, and why that order?
<details><summary>Answer</summary>Lock the separate <code>&lt;journal&gt;.lock</code> file (D65) → open the journal channel → check the header (D64) → cut a torn tail → write a header if the file is empty (<code>CommandJournal.java:71-103</code>). Lock first so no other process races; check before cut so a file that is not our journal is never modified; lock a separate file because on Linux/macOS closing any handle on the journal (which <code>read()</code> does) would drop a POSIX lock taken on the journal itself.</details>

7. `[07-exchange-service, 08]` Name the four `ExchangeException` reasons and say which ones mean "this request may be in the journal".
<details><summary>Answer</summary>BUSY and STOPPED: nothing journalled. HALTED: maybe journalled (resend with the same client order id after restart to find out, D49). INTERNAL_ERROR: it was journalled and then quarantined with a SKIP (<code>ExchangeException.java:5-14</code>).</details>

8. `[07-exchange-and-replay]` Why does replay make two passes over the file?
<details><summary>Answer</summary>Pass 1 verifies the whole history (contiguous sequence numbers, non-decreasing timestamps) and collects the SKIP set, which must be complete before feeding because a SKIP comes after the command it skips; pass 2 feeds. Streaming twice keeps memory constant (D60).</details>

9. `[07-gateway]` Why does the writer thread never block on a client?
<details><summary>Answer</summary>Replies and market data are only <code>offer</code>ed to a bounded per-session queue; if it is full the session is disconnected (D54, <code>ClientSession.java:77-86</code>). One slow client must not stall the exchange.</details>

10. `[10]` Why do `ExchangeServiceTest` assertions on `batches()` come after `stop()`?
<details><summary>Answer</summary>The writer updates the stats after the batch's futures complete (<code>ExchangeService.java:259-263</code>), so a test woken by a future can see stale values; <code>stop()</code> joins the writer, a happens-before edge. Without it, <code>waitingRequestsShareOneBatch</code> was flaky (Spec §10 #1).</details>

---

## (b) Predict, then verify

For each: write your prediction down, run it, then open the answer. The answers are the real outputs recorded on 2026-10-27 (timestamps and order ids depend on a fresh journal).

### b1. The README session `[05, 06, 07-gateway]`
Fresh journal. Window **alice**: `LOGIN alice`, `NEW a1 SELL LIMIT 10 100`, `SUBSCRIBE`. Window **bob**: `LOGIN bob`, `NEW b1 BUY LIMIT 4 101`. Predict every line each window prints.
<details><summary>Answer</summary>

```
alice < WELCOME participant=alice
alice < ORDER orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10
alice < SUBSCRIBED
alice < BOOK seq=1 last=0 bids=- asks=100x10
bob   < WELCOME participant=bob
bob   < FILL orderId=2 clOrdId=b1 side=BUY price=100 qty=4 leaves=0 liquidity=TAKER
bob   < ORDER orderId=2 clOrdId=b1 status=FILLED filled=4 remaining=0
alice < FILL orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER
alice < TRADE seq=2 price=100 qty=4 aggressor=BUY
alice < LEVEL seq=2 side=SELL price=100 qty=6 orders=1
```
</details>

### b2. Duplicate, band, protocol error `[07-engine, 07-gateway]`
Continue as bob: `NEW b1 BUY LIMIT 4 101`, then `NEW b2 BUY LIMIT 1 200`, then `NEW b3 BUY MARKET 1 100`. Predict each reply, including the `orderId` values.
<details><summary>Answer</summary>

```
ORDER orderId=2 clOrdId=b1 status=REJECTED filled=0 remaining=4 reason=DUPLICATE_CLIENT_ORDER_ID
ORDER orderId=4 clOrdId=b2 status=REJECTED filled=0 remaining=1 reason=PRICE_OUTSIDE_BAND
ERROR NEW takes 4 field(s), got 5
```
The duplicate answers with the <em>original</em> id 2, but used up id 3 (every new order consumes an id). 200 is outside ±10% of the last trade 100. A MARKET order has no price field, so the line fails parsing at the gateway and never reaches the journal.</details>

### b3. Cancels and market data `[05 flow 2, 07-book]`
bob: `CANCEL 1`. alice: `CANCEL 1`. Predict both replies and alice's market data. Then look at `study.journal`: how many records, and of which kinds?
<details><summary>Answer</summary>

```
bob   < CANCEL_REJECT orderId=1 reason=UNKNOWN_ORDER
alice < CANCELLED orderId=1 qty=6
alice < LEVEL seq=6 side=SELL price=100 qty=0 orders=0
```
Journal: `HEADER,2,1`, then NEW 1–4 (both rejects included: state-dependent rejects are journalled, D13), CANCEL 5 (bob's, also journalled), CANCEL 6. The protocol error is not there.</details>

### b4. QUIT right after NEW `[07-gateway, 09 D63]`
In one paste (or one TCP write): `LOGIN dave`, `NEW d1 BUY LIMIT 1 95`, `NEW d2 BUY LIMIT 1 96`, `QUIT`. Predict the order of the lines.
<details><summary>Answer</summary>

```
< WELCOME participant=dave
< ORDER orderId=… clOrdId=d1 status=RESTED …
< ORDER orderId=… clOrdId=d2 status=RESTED …
< BYE
```
Since D63 the session queues a read behind the two orders and sends BYE only from that read's callback. Before the fix, BYE could come first and the ORDER lines were lost (199 of 200 in the regression test).</details>

### b5. Crash and replay `[05 flow 4, 07-journal]`
Kill the server hard (close its window, or `taskkill /F`). Append half a record to the journal without a newline, e.g. in PowerShell:
`[IO.File]::AppendAllText("study.journal", "NEW,7,2026-10-27T12:", (New-Object Text.UTF8Encoding($false)))`.
Restart, log in as carol, `SUBSCRIBE`, `NEW c1 BUY LIMIT 5 99`. Predict the BOOK line, carol's orderId and the LEVEL `seq`.
<details><summary>Answer</summary>

```
< BOOK seq=6 last=100 bids=- asks=-
< ORDER orderId=5 clOrdId=c1 status=RESTED filled=0 remaining=5
< LEVEL seq=7 side=BUY price=99 qty=5 orders=1
```
The torn tail is cut silently (nothing printed: <code>Main</code> does not report it). The sequencer resumes from the last journalled values (seq 6, order id 4).</details>

### b6. Wrong rules version `[07-journal, 09 D46]`
Copy the journal, change its first line to `HEADER,2,9` keeping LF line endings, and start the server on the copy. Predict the output and exit code. Then compare with the original file.
<details><summary>Answer</summary>`Startup failed: java.lang.IllegalStateException: Journal … was written under rules version 9; this build uses 1. Start a fresh journal from a snapshot.` then the generic poison-pill hint; exit code 1. The file is unchanged (D64).</details>

### b7. Quarantine against a running server `[07-exchange-and-replay, 09 D65]`
With the server running on `study.journal`, run `java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal study.journal --quarantine 2`. Predict.
<details><summary>Answer</summary>`Quarantine failed: Journal … is already open (a running exchange, or a --quarantine run)`, exit 1, journal unchanged. Before D65 the tool appended a SKIP with its own sequence number next to the running server's.</details>

### b8. A JUnit prediction `[07-engine]`
Add temporarily to `MatchingEngineTest` and predict `status`, `filled`, the trade count, and what is left on the book:

```java
@Test
void studyMarketCollar() {
    limit("s", SELL, 100, 1);
    limit("b", BUY, 100, 1);                    // trade at 100: band reference = 100
    limit("s", SELL, 105, 2);
    limit("s", SELL, 111, 2);
    Response r = market("b", BUY, 10);
    System.out.println(r);
}
```
Run: `mvnw.cmd test -Dtest=MatchingEngineTest#studyMarketCollar` (nested classes need `MatchingEngineTest*` patterns; a top-level method works as shown).
<details><summary>Answer</summary>Trap: the sell at 111 arrives after the trade at 100, so the band applies to it: <code>111 × 100 = 11,100 > 100 × 110 = 11,000</code> → it is REJECTED <code>PRICE_OUTSIDE_BAND</code> and never rests. The market buy's collar is <code>100 × 110 / 100 = 110</code>; it takes 2 @ 105 and finds nothing else. <code>status=CANCELLED, filled=2, remaining=8</code>, one trade, empty ask side. (To see the collar stop a market order, rest the far ask <em>before</em> the first trade, as <code>MatchingEngineTest$Market.buyCollarStopsAtReferencePlusBand</code> does.)</details>

---

## (c) Tracing with a debugger

Use IntelliJ (File › Open › `pom.xml`) or VS Code with the Java extension. Debug a test, or start `Main` in debug mode with program arguments `--port 9000 --journal study.journal` and drive it with `ClientMain`. Thread names you will see: `exchange-writer`, `gateway-acceptor`, `session-N-reader`, `session-N-sender`, `ForkJoinPool.commonPool-worker-*`, `shutdown`.

c1. `[05 flow 1, 08]` **A NEW order end to end.** Breakpoints: `ClientSession.newOrder` (`src/main/java/exchangeserver/gateway/ClientSession.java:245`), `ExchangeService.enqueue` (`core/ExchangeService.java:182`), `Exchange.process` (`core/Exchange.java:118`), `CommandJournal.appendAll` (`journal/CommandJournal.java:114`), `MatchingEngine.process` (`engine/MatchingEngine.java:106`), `TcpGateway.onOutput` (`gateway/TcpGateway.java:183`). Send one crossing NEW. Record which thread hits each breakpoint, and the value of `p.command` before and after stamping.
<details><summary>What you should see</summary>The first two on <code>session-N-reader</code>; the rest on <code>exchange-writer</code>. <code>p.command</code> is null until <code>stampNewOrder</code>, then a <code>NewOrderCommand</code> with seq, timestamp, order id. <code>appendAll</code> returns before <code>MatchingEngine.process</code> is entered.</details>

c2. `[07-book]` **Watch a level empty.** Breakpoint in `OrderBook.removeNode` (`book/OrderBook.java:110`). Rest one sell, then send a buy that takes it fully. Step through `level.unlink`, then the `level.isEmpty()` branch. Watch `asks.size()` before and after, and the node's `prev/next/level` after unlink.
<details><summary>What you should see</summary>The node's three links become null; the level is removed from the TreeMap in the same call; <code>TreeMap.remove</code> returned that same level (D18).</details>

c3. `[07-exchange-and-replay]` **A poison pill.** Debug `ExchangeTest$PoisonPill.poisonInTheMiddleOfABatchDoesNotDisturbTheOthers`. Breakpoint in `Exchange.quarantine` (private, `core/Exchange.java:178`). Inspect `poison.sequenceNumber()`, the SKIP's sequence number, and the `skipped` set before `Replay.feed`.
<details><summary>What you should see</summary>Poison seq 2, SKIP stamped as seq 4 (the batch had used 1–3), <code>skipped = {2}</code>; the rebuilt engine is fed only seq 1; the batch then continues with seq 3 on the rebuilt engine.</details>

c4. `[07-exchange-service]` **Stop draining.** Debug `ExchangeServiceTest$Stop.drainsEverythingAcceptedAndRefusesTheRest`. Breakpoints at `ExchangeService.stop` (`core/ExchangeService.java:208`) and at `stopping = batch.remove(STOP)` (`:258`). Confirm STOP is the last element of the batch it appears in.

c5. `[07-gateway]` **Subscribe race.** In `ClientSession.subscribe` (`gateway/ClientSession.java:276`) set a breakpoint inside the snapshot callback, then from the client send `SUBSCRIBE` and `UNSUBSCRIBE` quickly. Observe `mine` vs `subscriptionNumber` and `subscriptionActive` when the callback runs.

---

## (d) Small hands-on changes

Each on its own branch. "Done" means: the new tests pass, `mvnw.cmd test` is green, no compiler warnings.

d1. `[12 Recipe 5, 07-gateway]` **Add `TIME` → `TIME seq=<last seq>`**, answered through the queue (snapshot depth 1, callback variant). Done: `ProtocolTest` parse + format tests, one `TcpGatewayTest` case asserting the seq after two orders is 2.

d2. `[12 Recipe 7, 07-client-and-main]` **Add `--queue-capacity <n>` to `Main`.** Done: invalid values print the usage line and exit 1; README/`CLAUDE.md` updated. (No unit test exists for `Main`; verify by hand.)

d3. `[12 Recipe 3, 07-domain-and-commands]` **Change the band to 5%.** Bump `Rules.VERSION`, fix the tests that encode 10% (`RulesTest`, `MatchingEngineTest.Risk`, `MatchingEngineTest.Market`). Done: green, and starting on an old journal is refused.

d4. `[11, 08]` **Report replay at startup.** Make `Main` print "replayed N commands, cut M torn bytes". `ExchangeService` does not expose its `Exchange`, so decide where the numbers come from (add accessors? log inside `Exchange.open`?). Done: the line appears; Spec §8's "Startup reporting" thread updated.

d5. `[12 Recipe 2]` **New reject: `QUANTITY_ABOVE_BOOK`** for an IOC larger than the whole opposite side. Done: the reject is journalled (an `ExchangeTest` that restarts and sees the same outcome), `Rules.VERSION` bumped, PIT shows no survivor in your new branch of `process`.

d6. `[10]` **Write the missing test for "no self-trade prevention".** Assert that a participant's buy matches its own resting sell and both FILL lines go to the same session. Done: test green, and the Spec's open thread links to it.

d7. `[04, 07-journal, 15]` **Run the journal tests on Linux.** Start Docker Desktop, then from the repo root: `docker run --rm -v "$PWD":/w -w /w eclipse-temurin:17 ./mvnw -B test -Dtest=CommandJournalTest` (in PowerShell use `${PWD}`; if `mvnw` is not executable inside the container, prefix it with `sh`). Done: you know whether D65's cross-process test passes on Linux, and guide 15 item 18 can be closed or reopened. The Maven download inside the container takes a while the first time.

---

## (e) "Break it" experiments

For each: predict what breaks and which test catches it, make the change on a branch, run `mvnw.cmd clean test` (or the named class), compare with the answer, revert.

e1. `[07-exchange-and-replay, 09 D40]` In `Exchange.process`, move `journal.appendAll(commands)` (`core/Exchange.java:137`) to after the processing loop.
<details><summary>Answer</summary>Replies become visible before they are durable. <code>ExchangeTest$LivePath.visibleImpliesDurable</code> is the guard; poison-pill tests also change behaviour because the SKIP would precede its target's record. A crash between processing and appending would leave clients with fills the journal never heard of.</details>

e2. `[07-exchange-and-replay, 09 D61]` Delete the `if (p.refused) continue;` (`core/Exchange.java:142-144`).
<details><summary>Answer</summary>A refused request (e.g. a cancel with order id 0) has <code>p.command == null</code>. It reaches <code>apply(null)</code>, which throws <code>IllegalStateException("Not an engine command: null")</code>; that is caught as a poison pill, and <code>quarantine(null, …)</code> then throws a <code>NullPointerException</code> (it reads <code>poison.sequenceNumber()</code>) out of <code>process</code>. In <code>ExchangeService</code> that halts the writer. <code>ExchangeTest$LivePath.invalidCancelIsRefusedWithoutUsingASequenceNumber</code> fails. Lesson: the writer must decide from its own state (D61).</details>

e3. `[07-engine, 09 D12]` Make `MatchingEngine` stamp trades with `Instant.now()` instead of `order.getTimestamp()` (`engine/MatchingEngine.java:148`).
<details><summary>Answer</summary><code>MatchingEngineTest$Outputs.tradeTimestampIsTheIncomingCommandsTimestamp</code> fails; <code>Determinism.sameCommandsGiveIdenticalOutputs</code> almost always fails (two runs see different instants). The real damage: replay would produce different trade records than clients saw.</details>

e4. `[07-journal, 08]` Delete `channel.force(true)` in `appendAll` (`journal/CommandJournal.java:131`).
<details><summary>Answer</summary>No test fails. An fsync is not observable without pulling the power; PIT lists it as an expected survivor (Spec §7). Durability is now a lie after an OS crash, but not after a process crash (the OS still has the bytes).</details>

e5. `[07-domain-and-commands, 09 D46]` Change `PRICE_BAND_PERCENT` to 5 **without** bumping `VERSION`, and restart on a journal containing an order at +8% of the last trade.
<details><summary>Answer</summary>Startup succeeds, but the replayed order is now REJECTED instead of RESTED or traded: the rebuilt book silently differs from what clients saw. Tests that hard-code 10% fail; no test detects the missing bump. This is exactly why the version exists.</details>

e6. `[07-journal, 11]` Convert a journal to CRLF (open in an editor that saves CRLF, or `(Get-Content j) | Set-Content j2 -Encoding ascii` in Windows PowerShell) and start on it.
<details><summary>Answer</summary>Startup fails: <code>Bad journal header "HEADER,2,1\r": For input string: "1\r"</code>, exit 1 (observed with version 9). Records are LF-only; nothing strips a trailing <code>\r</code>. Spec §8 lists it as a known limitation.</details>

e7. `[07-gateway, 09 D63]` Revert D63: in `ClientSession.handle` send `"BYE"` and return `false` on QUIT, and in `readLoop` call `close()` directly in `finally`. Run `mvnw.cmd clean test -Dtest=TcpGatewayTest`.
<details><summary>Answer</summary><code>quitAnswersEveryEarlierRequestBeforeBye</code> fails (observed: 1 ORDER line instead of 200) and <code>endOfStreamStillGetsTheReplies</code> fails (0 instead of 50).</details>

e8. `[07-journal, 09 D65]` Remove the `Lock.acquire(file)` call in `CommandJournal.open` (and the matching releases).
<details><summary>Answer</summary><code>CommandJournalTest$Exclusive.aSecondOpenIsRefusedUntilTheFirstCloses</code>, <code>anotherProcessIsRefusedWhileTheJournalIsOpen</code> (it runs <code>Main --quarantine</code> in a child JVM) and <code>ExchangeTest$PoisonPill.quarantineToolRefusesWhileTheExchangeIsRunning</code> fail.</details>

e11. `[07-journal, 09 D65, 15]` **The POSIX trap.** Change `Lock.acquire` to lock the *journal file itself* (open the journal path instead of `<journal>.lock`), keep the `HELD` set, and run `CommandJournalTest` twice: on Windows (`mvnw.cmd test -Dtest=CommandJournalTest`) and on Linux in Docker (`docker run --rm -v "$PWD":/w -w /w eclipse-temurin:17 ./mvnw -B test -Dtest=CommandJournalTest`, from the repo root; Docker Desktop must be running).
<details><summary>Answer</summary>[Inferred, not yet run] Windows: probably still green, because Windows locks belong to the handle (although a mandatory lock on the records may now block <code>read()</code> and the torn-tail cut, which use their own handles: watch for "another process has locked a portion of the file"). Linux: <code>anotherProcessIsRefusedWhileTheJournalIsOpen</code> should fail. The test calls <code>read()</code> before starting the child, and closing that read's handle drops the process's POSIX lock on the journal, so the child's <code>--quarantine</code> succeeds and appends a SKIP. That is the reason D65 locks a separate file that nothing else ever opens or closes.</details>

e9. `[07-exchange-service, 08]` Make `ExchangeService.state` non-`volatile` and remove the `synchronized (lock)` in `enqueue`.
<details><summary>Answer</summary>Usually nothing fails — data races are rarely reproducible on x86 in a short run. <code>racingStopNeverLosesARequest</code> (5 repetitions) is the test most likely to catch a lost request, by luck. The lesson: concurrency correctness comes from the happens-before argument, not from tests.</details>

e10. `[07-book]` In `OrderBook.add`, put the new level into the TreeMap *before* `level.addOrder(order)` (`book/OrderBook.java:62-68`).
<details><summary>Answer</summary>If <code>addOrder</code> threw, an empty level would be left behind (violating "no empty level"). <strong>[Inferred]</strong> Probably no test fails: <code>add</code> already refuses non-resting types, exhausted orders and duplicate ids before creating the level, and a brand-new level cannot overflow, so <code>addOrder</code> cannot throw on a fresh level today. The ordering is defence in depth (D10/D14: "a failed add must not leave an empty level", <code>OrderBook.java:67</code>), the kind of thing PIT reports as an equivalent mutant (Spec §7).</details>

---

## Key takeaways

- Predict before you run: every answer here can be derived from the code paths in guide 05.
- The live server, `ClientMain` and a journal file are the fastest feedback loop for protocol and replay questions.
- Debugger thread names tell you which side of the queue you are on: `session-N-reader` before, `exchange-writer` after.
- Some guarantees have no test that can fail (fsync, data races), and some are tested on one OS only (the cross-process journal lock: Windows); know which.
- Version numbers exist because some mistakes (e5) are silent.

## Common confusions

- **"The duplicate's orderId=2 means no id was used."** Id 3 was used; the reply carries the original's id.
- **"A protocol ERROR is a reject."** It never reaches the queue or the journal; a reject is journalled.
- **"If the tests pass, the concurrency is right."** See e9.

## Questions to test yourself

1. Which exercise shows a mistake no test catches, and what would catch it in production?
<details><summary>Answer</summary>e4 (missing fsync) and e5 (rule change without version bump). An OS crash/power loss test rig would catch e4; only discipline or a check comparing replayed outputs with recorded outputs would catch e5.</details>

2. In b2, why is the band reject's orderId 4 and not 3?
<details><summary>Answer</summary>The duplicate reject was command 3 and consumed order id 3, even though it reported the original id 2.</details>

3. In c1, why does `appendAll` return before `MatchingEngine.process` is entered, even for a batch of one?
<details><summary>Answer</summary>Write-ahead: <code>Exchange.process</code> appends and fsyncs the whole batch first, then processes; nothing is visible before it is durable.</details>
