# 14 — Exercises

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** active learning from easy to hard: self-check questions, predict-then-verify runs, debugger traces, hands-on changes and break-it experiments | **Prerequisites:** 01, 05 (do the rest as each exercise's tags say) | **Related:** all guides, especially 05, 07, 10, 11, 12
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

Five kinds of exercise, easiest first, each tagged with the guides it reinforces, e.g. `[05, 07-engine]`. Answers are hidden in `<details>`: commit to an answer before you open one.

**Setup used throughout** (Windows; on macOS/Linux use `./mvnw` and `/`):

```
mvnw.cmd -q package -DskipTests
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --users study.users --add-user alice     (repeat for bob, carol, dave)
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal study.journal --users study.users
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000
```

`--add-user` asks for the secret twice on a terminal, or reads one line from standard input **[Verified]** (`src/main/java/exchangeserver/Main.java:212-235`). Below, `<secret>` means the secret you chose for that participant. `ClientMain` sends what you type and prints server lines with `< ` **[Verified]** (`src/main/java/exchangeserver/client/ClientMain.java`). Delete `study.journal*` to start fresh (git-ignored). Stop with Ctrl+C: exit code 0 **[Verified on Linux]** (SIGINT and SIGTERM, Spec §11).

For code changes: a throwaway branch (`git switch -c study/<name>`); after swapping files around build with `mvnw.cmd clean …` (incremental compilation goes by timestamp, `CLAUDE.md`). Never run two Maven builds at once.

---

## (a) Self-check questions

1. `[01, 05]` In what order does the writer thread do these for a batch: stamp, fsync, engine, listener, future?
<details><summary>Answer</summary>A CHECK if due, then stamp every request → append all with one fsync → for each: ledger (engine, digest, private events) → OutputListener → complete the future/callback (D76). <code>src/main/java/exchangeserver/core/Exchange.java:198-253</code>.</details>

2. `[07-domain-and-commands]` Which single class reads the clock? What happens if the clock steps back?
<details><summary>Answer</summary><code>command.Sequencer</code>; it reuses the previous timestamp, so journal timestamps never decrease (D33). <code>src/main/java/exchangeserver/command/Sequencer.java:86-93</code>.</details>

3. `[07-book]` Why does a `PriceLevel` need sentinel nodes?
<details><summary>Answer</summary>Every real node always has non-null <code>prev</code>/<code>next</code>, so append and unlink need no edge-case branches.</details>

4. `[07-engine, 06]` A BUY LIMIT 4 @ 101 from bob hits alice's ask of 10 @ 100. Price, and who gets which lines?
<details><summary>Answer</summary>At 100 (the resting price). bob's private stream: <code>FILL oseq=… side=BUY price=100 qty=4 leaves=0 liquidity=TAKER</code>, then <code>ORDER oseq=… status=FILLED</code>; alice's stream: <code>FILL oseq=… leaves=6 liquidity=MAKER</code> — to every session of each participant (D73); subscribers: <code>TRADE</code> and <code>LEVEL … qty=6</code>.</details>

5. `[07-engine, 09]` Why is a cancel of someone else's order answered `UNKNOWN_ORDER`?
<details><summary>Answer</summary>D48: a distinct status would let any participant probe which order ids are live.</details>

6. `[07-journal]` What does `CommandJournal.open` do, in order?
<details><summary>Answer</summary>Take the <code>JournalLock</code> (<code>&lt;journal&gt;.lock</code>, D65) → check the header (refuse a foreign file or other rules with records; let an empty journal adopt new rules, D64, D71) → open the journal channel → cut a torn tail → write a header if empty and fsync the directory (D69). <code>src/main/java/exchangeserver/journal/CommandJournal.java:75-128</code>.</details>

7. `[07-exchange-service, 08]` Which refusals mean "this request may be in the journal"?
<details><summary>Answer</summary>BUSY, STOPPED, RATE_LIMITED, SESSION_BUSY: never journalled. HALTED: maybe (resend with the same client order id, or RESEND the private stream after restart). INTERNAL_ERROR: journalled, then quarantined.</details>

8. `[07-exchange-and-replay]` Where does replay start, and what does it compare along the way?
<details><summary>Answer</summary>From the snapshot the header's base names (or empty), then the journal: pass 1 verifies sequence from base + 1 and timestamps, pass 2 feeds the ledger and compares every CHECK digest (D70, D71). <code>src/main/java/exchangeserver/core/Replay.java:39-99</code>.</details>

9. `[07-gateway]` Name the three per-session limits and what each answers.
<details><summary>Answer</summary>Login/idle timeouts (<code>ERROR login timeout; disconnecting</code> / <code>idle timeout</code>), in-flight cap (<code>SESSION_BUSY</code>), rate limit (<code>RATE_LIMITED</code>) — D75, <code>src/main/java/exchangeserver/gateway/ClientSession.java:279-291</code>.</details>

10. `[07-gateway, 09]` Why does a session's request callback run *after* the listener?
<details><summary>Answer</summary>So when the last callback brings the in-flight count to 0 and the session closes, the request's own lines were already offered (D76). The old D63 barrier read was refused during shutdown and lost replies.</details>

---

## (b) Predict, then verify

Write your prediction down, run it, then open the answer.

### b1. The README session `[05, 06, 07-gateway]`
Fresh journal. **alice**: `LOGIN alice <secret>`, `NEW a1 SELL LIMIT 10 100`, `SUBSCRIBE`. **bob**: `LOGIN bob <secret>`, `NEW b1 BUY LIMIT 4 101`. Predict every line.
<details><summary>Answer</summary>

```
alice < WELCOME participant=alice oseq=0
alice < ORDER oseq=1 orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10
alice < SUBSCRIBED
alice < BOOK seq=1 last=0 bids=- asks=100x10
bob   < WELCOME participant=bob oseq=0
bob   < FILL oseq=1 orderId=2 clOrdId=b1 side=BUY price=100 qty=4 leaves=0 liquidity=TAKER
bob   < ORDER oseq=1 orderId=2 clOrdId=b1 status=FILLED filled=4 remaining=0
alice < FILL oseq=2 orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER
alice < TRADE seq=2 price=100 qty=4 aggressor=BUY
alice < LEVEL seq=2 side=SELL price=100 qty=6 orders=1
```
<strong>[Verified]</strong> (<code>TcpGatewayTest.Trading.restThenCrossFillsBothSides</code>, <code>MarketData.snapshotThenIncrementalUpdates</code>; a live run on 2026-10-27 printed the same lines). Each participant's oseq starts at 1.</details>

### b2. Wrong secret `[07-gateway, 09 D74]`
New window: `LOGIN alice guessed-secret`. Then try `PING`.
<details><summary>Answer</summary><code>ERROR login failed</code>, and the connection closes: one attempt per connection, and the same answer whether the name or the secret was wrong <strong>[Verified]</strong> (live run; <code>TcpGatewayTest.Security</code>).</details>

### b3. Duplicate, band, protocol error `[07-engine, 07-gateway]`
Continue as bob: `NEW b1 BUY LIMIT 4 101`, `NEW b2 BUY LIMIT 1 200`, `NEW b3 BUY MARKET 1 100`.
<details><summary>Answer</summary>

```
ORDER oseq=2 orderId=2 clOrdId=b1 status=REJECTED filled=0 remaining=4 reason=DUPLICATE_CLIENT_ORDER_ID
ORDER oseq=3 orderId=4 clOrdId=b2 status=REJECTED filled=0 remaining=1 reason=PRICE_OUTSIDE_BAND
ERROR NEW takes 4 field(s), got 5
```
The duplicate reports the original id 2 but used up id 3. 200 is outside ±10% of 100. A MARKET order has no price field: refused at the gateway, never journalled. <strong>[Inferred from code]</strong> (the same session without oseq was run live before D73).</details>

### b4. Cancels and the journal `[05 flow 2, 07-journal]`
bob: `CANCEL 1`. alice: `CANCEL 1`. Then open `study.journal`.
<details><summary>Answer</summary>

```
bob   < CANCEL_REJECT oseq=4 orderId=1 reason=UNKNOWN_ORDER
alice < CANCELLED oseq=3 orderId=1 qty=6
alice < LEVEL seq=6 side=SELL price=100 qty=0 orders=0
```
Journal: <code>HEADER,3,2,0,&lt;crc&gt;</code>, NEW 1–4 (both rejects too, D13), CANCEL 5 and 6 — every line ending in <code>,&lt;8 hex digits&gt;</code> (D69). bob's rejected cancel consumed his oseq 4.</details>

### b5. Self-trade prevention `[07-engine, 09 D67]`
Fresh journal. x sells 2 @ 100 (`NEW x1 SELL LIMIT 2 100`), alice sells 3 @ 100, y sells 5 @ 100 (use three participants). alice: `NEW a9 BUY LIMIT 9 100`. Then a second case: x sells 2, y sells 3, alice sells 5 (alice last); alice buys 4 @ 100.
<details><summary>Answer</summary>Case 1: alice trades 2 with x, then the next order in time priority is her own: matching stops; <code>ORDER … status=CANCELLED filled=2 remaining=7 reason=SELF_TRADE_PREVENTED</code>; y's order is never reached (STP stops, it does not skip). Case 2: she takes 2 from x and 2 from y and never reaches her own: <code>FILLED</code>. <strong>[Verified]</strong> (<code>MatchingEngineTest.SelfTrade.tradesWithOthersAheadThenStopsAtItsOwnOrder</code>; case 2 by the same code path).</details>

### b6. QUIT right after NEW `[07-gateway, 09 D76]`
In one paste: `LOGIN dave <secret>`, `NEW d1 BUY LIMIT 1 95`, `NEW d2 BUY LIMIT 1 96`, `QUIT`.
<details><summary>Answer</summary>

```
< WELCOME participant=dave oseq=0
< ORDER oseq=1 orderId=… clOrdId=d1 status=RESTED …
< ORDER oseq=2 orderId=… clOrdId=d2 status=RESTED …
< BYE
```
The session counts the two orders in flight and sends BYE when the last callback ran (D76). <strong>[Verified]</strong> (<code>TcpGatewayTest.Session.quitAnswersEveryEarlierRequestBeforeBye</code>; live before oseq existed).</details>

### b7. RESEND after a disconnect `[07-gateway, 09 D73]`
alice (logged in with `COD`): `NEW a1 SELL LIMIT 10 100`, `NEW a2 BUY LIMIT 5 100`; bob buys 4 @ 100; alice `QUIT`. Then a new alice window: `LOGIN alice <secret>`, `RESEND 1`.
<details><summary>Answer</summary>

```
alice2 < WELCOME participant=alice oseq=4
alice2 < ORDER oseq=1 orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10 resend=1
alice2 < ORDER oseq=2 orderId=2 clOrdId=a2 status=CANCELLED filled=0 remaining=5 reason=SELF_TRADE_PREVENTED resend=1
alice2 < FILL oseq=3 orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER resend=1
alice2 < CANCELLED oseq=4 orderId=1 qty=6 reason=CANCEL_ALL resend=1
alice2 < RESENT last=4
```
a2 would have crossed her own ask (STP). On QUIT her last COD session closed, so the gateway submitted CANCEL_ALL (event 4). <strong>[Verified]</strong> (live run, 2026-10-27).</details>

### b8. Crash and replay `[05 flow 4, 07-journal]`
After b4, kill the server hard (`taskkill /F` or close its window). Append half a record: `[IO.File]::AppendAllText("study.journal", "NEW,7,2026-10-27T12:", (New-Object Text.UTF8Encoding($false)))`. Restart. Predict the startup lines; then carol: `SUBSCRIBE`, `NEW c1 BUY LIMIT 5 99`.
<details><summary>Answer</summary>

```
ExchangeServer2 listening on 127.0.0.1:9000, journal …\study.journal
Recovered through sequence 6: replayed 6 commands, cut 20 torn bytes
< BOOK seq=6 last=100 bids=- asks=-
< ORDER oseq=1 orderId=5 clOrdId=c1 status=RESTED filled=0 remaining=5
< LEVEL seq=7 side=BUY price=99 qty=5 orders=1
```
The torn bytes are cut and now reported (D77). <strong>[Inferred]</strong> from code (the same steps were run live before D77 added the summary line).</details>

### b9. Editing the header by hand `[07-journal, 09 D69]`
Copy the journal and change the header's rules version from 2 to 9 in a text editor that keeps LF. Start on the copy.
<details><summary>Answer</summary><code>Startup failed: Bad journal header "HEADER,3,9,0,…": checksum mismatch (expected …, found …)</code> and <code>The journal was not changed.</code>, exit 1. The CRC covers the header too, so a hand edit is caught before the version check. (To really test a version mismatch, use <code>CommandJournal.writeFresh</code> with another version, as <code>CommandJournalTest</code> does.) <strong>[Inferred from code]</strong>, <code>src/main/java/exchangeserver/journal/JournalCodec.java:63-85</code>, <code>:171-187</code>.</details>

### b10. Tools against a running server `[07-exchange-and-replay, 09 D65]`
With the server running: `--journal study.journal --quarantine 2`, then `--compact`.
<details><summary>Answer</summary><code>Quarantine failed: Journal … is already open (a running exchange, or a --quarantine or --compact run)</code> and the same for <code>Compaction failed:</code>, exit 1, nothing written. <strong>[Verified]</strong> live on Windows and (quarantine) on Linux.</details>

### b11. Compaction and restart `[05 flow 4, 09 D71]`
Stop the server, run `--journal study.journal --compact`, list the files, start again.
<details><summary>Answer</summary><code>Compacted through sequence N: K resting orders in study.journal.snapshot-N; old journal archived as study.journal.archive-1-N</code>; files: <code>study.journal</code> (one header line, base N), the archive, the snapshot, the lock file. Restart prints <code>Recovered through sequence N from snapshot N: replayed 0 commands, cut 0 torn bytes</code>. A second <code>--compact</code> says nothing to compact. <strong>[Verified]</strong> (live run with N = 4).</details>

### b12. A JUnit prediction: the collar `[07-engine]`
Add temporarily to `MatchingEngineTest` and predict status, filled and the book:

```java
@Test
void studyCollarBeforeFirstTrade() {
    limit("s", SELL, 100, 1);
    limit("s", SELL, 110, 1);
    limit("s", SELL, 500, 1);
    System.out.println(market("b", BUY, 3));
}
```
Run `mvnw.cmd test -Dtest=MatchingEngineTest#studyCollarBeforeFirstTrade`.
<details><summary>Answer</summary>No trade yet, so the collar is anchored on the touch (100): buys up to 110. It takes 100 and 110, then stops: <code>CANCELLED filled=2 remaining=1</code>; 500 stays. Before D66 it swept all three. <strong>[Verified]</strong> (<code>MatchingEngineTest.Market.beforeTheFirstTradeTheCollarIsAnchoredOnTheTouch</code>).</details>

---

## (c) Tracing with a debugger

IntelliJ (File › Open › `pom.xml`) or VS Code with the Java extension. Threads you will see: `exchange-writer`, `gateway-acceptor`, `session-N-reader`, `session-N-sender`, `ForkJoinPool.commonPool-worker-*`, `shutdown`.

c1. `[05 flow 1, 08]` **A NEW order end to end.** Breakpoints: `ClientSession.newOrder` (`src/main/java/exchangeserver/gateway/ClientSession.java:329`), `ExchangeService.enqueue` (`src/main/java/exchangeserver/core/ExchangeService.java:211`), `Exchange.process` (`src/main/java/exchangeserver/core/Exchange.java:198`), `CommandJournal.appendAll` (`src/main/java/exchangeserver/journal/CommandJournal.java:143`), `MatchingEngine.process` (`src/main/java/exchangeserver/engine/MatchingEngine.java:178`), `TcpGateway.onOutput` (`src/main/java/exchangeserver/gateway/TcpGateway.java:286`). Send one crossing NEW. Which thread hits each? What is in `events` at `onOutput`?
<details><summary>What you should see</summary>The first two on <code>session-N-reader</code>, the rest on <code>exchange-writer</code>. <code>appendAll</code> returns before the engine runs. <code>events</code> holds an <code>OrderReply</code> for the taker and a <code>MakerFill</code> per passive fill, each with its participant's next oseq.</details>

c2. `[07-book]` **Watch a level empty.** Breakpoint in `OrderBook.removeNode` (`src/main/java/exchangeserver/book/OrderBook.java:111`). Rest one sell, take it fully; step through `unlink` and the empty-level removal.

c3. `[07-exchange-and-replay]` **A poison pill.** Debug `ExchangeTest$PoisonPill.poisonInTheMiddleOfABatchDoesNotDisturbTheOthers`; breakpoint in the private `Exchange.quarantine` (`src/main/java/exchangeserver/core/Exchange.java:278`).
<details><summary>What you should see</summary>Poison seq 2, SKIP stamped as seq 4, <code>skipped = {2}</code>; <code>Replay.rebuild</code> builds a new ledger from the snapshot (none here) plus seq 1; the batch continues with seq 3.</details>

c4. `[07-exchange-service]` **Stop draining.** Debug `ExchangeServiceTest$Stop.drainsEverythingAcceptedAndRefusesTheRest`; breakpoints at `stop` (`src/main/java/exchangeserver/core/ExchangeService.java:245`) and `stopping = batch.remove(stopMarker)` (`:298`).

c5. `[07-gateway, 09 D76]` **A draining session.** Breakpoint in `ClientSession.finishIfIdle` (`src/main/java/exchangeserver/gateway/ClientSession.java:434`). Paste `NEW …` and `QUIT` together. Watch `readerDone`, `inFlight` and which thread finally passes the `compareAndSet`.
<details><summary>What you should see</summary>The reader calls it first with <code>inFlight</code> 1 (nothing happens); the writer thread's callback calls it with 0 and closes, after the ORDER line was offered.</details>

c6. `[07-ledger-and-snapshots]` **Replay compares a CHECK.** Debug `ExchangeTest$Checkpoints.aCheckIsWrittenEveryIntervalAndReplayAcceptsIt` with a breakpoint in `Replay.feed` at the CHECK branch (`src/main/java/exchangeserver/core/Replay.java:80-88`). Compare `check.digest()` with `ledger.digest()`.

---

## (d) Small hands-on changes

Each on its own branch. "Done": new tests pass, `mvnw.cmd test` green, no compiler warnings.

d1. `[12 Recipe 5, 07-gateway]` **Add `TIME` → `TIME seq=<last seq>`** through the queue (snapshot depth 1, callback via `tracked`). Done: `ProtocolTest` parse/format and a `TcpGatewayTest` case.

d2. `[12 Recipe 8, 07-client-and-main]` **Add `--queue-capacity <n>`** to `Main`. Done: bad values print the usage line and exit 1; README and `CLAUDE.md` updated.

d3. `[12 Recipe 3]` **Change the band to 5%.** Bump `Rules.VERSION`, fix the tests that encode 10%. Then carry a journal across: compact with the old jar, start the new one. Done: green, and `Recovered … journal moved from rules 2` is printed.

d4. `[07-ledger-and-snapshots]` **See CHECK records.** Start with `--check-interval 2`, place five orders, read the journal. Done: you can say why each CHECK has the sequence number it has.

d5. `[12 Recipe 2]` **New reject `QUANTITY_ABOVE_BOOK`** for an IOC larger than the whole opposite side. Done: journalled and replayed (`ExchangeTest`), `Rules.VERSION` bumped, PIT shows no survivor in the new branch.

d6. `[12 Recipe 2, 09 D67]` **Change STP to "cancel the resting order"** (cancel own resting orders it meets and keep matching). Done: `MatchingEngineTest.SelfTrade` rewritten for the new policy, a D78 written, `Rules.VERSION` bumped.

d7. `[04, 12 Recipe 13]` **Run the suite on Linux** with the Docker command in guide 12 Recipe 13. Done: 446-plus tests green in the container.

---

## (e) "Break it" experiments

Predict what breaks and which test catches it; change it on a branch; run `mvnw.cmd clean test`; revert.

e1. `[07-exchange-and-replay, 09 D40]` Move `journal.appendAll(commands)` (`src/main/java/exchangeserver/core/Exchange.java:223`) after the processing loop.
<details><summary>Answer</summary>Replies become visible before they are durable; <code>ExchangeTest$LivePath.visibleImpliesDurable</code> is the guard; poison-pill tests break because the SKIP would precede its target.</details>

e2. `[07-exchange-and-replay, 09 D61]` Delete `if (p.refused) continue;` (`src/main/java/exchangeserver/core/Exchange.java:228-230`).
<details><summary>Answer</summary>A refused request has <code>p.command == null</code>, is treated as a read, and <code>read(...)</code> throws "Not a read" out of <code>process</code> — the writer halts. <code>ExchangeTest$LivePath.invalidCancelIsRefusedWithoutUsingASequenceNumber</code> fails. <strong>[Inferred]</strong></details>

e3. `[07-engine, 09 D12]` Stamp trades with `Instant.now()` instead of `order.getTimestamp()` (`src/main/java/exchangeserver/engine/MatchingEngine.java:234`).
<details><summary>Answer</summary><code>MatchingEngineTest$Outputs.tradeTimestampIsTheIncomingCommandsTimestamp</code> fails; determinism tests usually fail. Trade timestamps are not in the digest, so replay would not stop — another reason the rule matters.</details>

e4. `[07-journal, 08]` Delete `channel.force(true)` in `appendAll` (`src/main/java/exchangeserver/journal/CommandJournal.java:160`).
<details><summary>Answer</summary>No test fails: an fsync is not observable without pulling the power (an expected PIT survivor, Spec §7).</details>

e5. `[07-domain-and-commands, 09 D70]` Change `PRICE_BAND_PERCENT` to 5 **without** bumping `VERSION`, and restart on a journal with an order at +8% of the last trade, started with `--check-interval 1`.
<details><summary>Answer</summary>The replayed order is now rejected instead of resting, so the next CHECK's digest differs: startup fails with <code>Replay diverged before CHECK …</code> and the compaction advice. With the default interval (10,000) a small journal has no CHECK yet, and the change goes unnoticed — the digest only catches what a CHECK covers.</details>

e6. `[07-journal, 09 D69]` Convert a journal to CRLF (`(Get-Content j) | Set-Content j2 -Encoding ascii` in Windows PowerShell) and start on it.
<details><summary>Answer</summary>Startup fails with <code>record ends with '\r': the file was converted to CRLF line endings; convert it back to LF (e.g. dos2unix) and restart</code>, exit 1. <strong>[Verified]</strong> (<code>JournalCodecTest.Corrupt.aCarriageReturnIsReportedAsALineEndingConversion</code>; a test that wrote a journal with <code>Files.write</code> hit exactly this on Windows).</details>

e7. `[07-exchange-and-replay, 09 D76]` In `Exchange.process`, swap the listener call and `p.complete(...)` (`src/main/java/exchangeserver/core/Exchange.java:246-251`).
<details><summary>Answer</summary><code>ExchangeServiceTest$HandIn.listenerRunsOnTheWriterBeforeTheCallback</code> fails. In the gateway, the last request's callback can close a QUITting session before its ORDER line is offered, so <code>TcpGatewayTest$Session.quitAnswersEveryEarlierRequestBeforeBye</code> and <code>endOfStreamStillGetsTheReplies</code> lose lines. <strong>[Inferred]</strong> for the gateway tests.</details>

e8. `[07-journal, 09 D65]` Remove the `JournalLock.acquire` in `CommandJournal.open`.
<details><summary>Answer</summary><code>CommandJournalTest$Exclusive</code> (both tests, including the child-JVM one) and <code>ExchangeTest$PoisonPill.quarantineToolRefusesWhileTheExchangeIsRunning</code> fail.</details>

e9. `[07-journal, 09 D65]` **The POSIX trap.** Make `JournalLock` lock the journal file itself and run `CommandJournalTest` on Windows and on Linux.
<details><summary>Answer</summary><strong>[Inferred, not run]</strong> Windows: likely green (locks belong to the handle) unless the mandatory lock blocks <code>read()</code>. Linux: <code>anotherProcessIsRefusedWhileTheJournalIsOpen</code> should fail, because closing the handle <code>read()</code> used drops the process's POSIX lock. The current design passes on both (Spec §11).</details>

e10. `[07-engine, 09 D67]` Remove the STP check in the match loop (`src/main/java/exchangeserver/engine/MatchingEngine.java:224-227`).
<details><summary>Answer</summary><code>MatchingEngineTest$SelfTrade</code> tests that expect CANCELLED/<code>SELF_TRADE_PREVENTED</code> after matching fail (the order now trades with itself), and <code>TcpGatewayTest$Trading.selfTradeIsPreventedOverTheWire</code> fails. The FOK tests still pass: <code>fillability</code> stops at the own order on its own. <strong>[Inferred]</strong></details>

e11. `[07-ledger-and-snapshots, 09 D70]` Remove the CHECK comparison in `Replay.feed` (`src/main/java/exchangeserver/core/Replay.java:80-88`; keep the `return`).
<details><summary>Answer</summary><code>ReplayTest$Check.aDifferentDigestStopsReplayAsDivergence</code>, <code>ExchangeTest$Checkpoints.aChangedHistoryIsCaughtAtTheNextCheck</code> and <code>MainTest.aReplayDivergenceGetsTheCompactionAdvice</code> fail.</details>

e12. `[07-journal, 08]` Remove every call to `Durable.fsyncDirectory`.
<details><summary>Answer</summary>No test fails. A lost directory entry only shows after a crash on Linux right after a file was created or renamed; on Windows the call is a no-op. PIT lists these as expected survivors (Spec §7).</details>

e13. `[04, 09 D69]` Remove the `*.journal -text` lines from `.gitattributes`, set `core.autocrlf=true`, force-add a journal (`git add -f`, it is git-ignored), commit, delete it and check it out again; start on it.
<details><summary>Answer</summary><strong>[Inferred]</strong> The checkout converts LF to CRLF; startup fails with the CRLF message of e6. With <code>-text</code> git leaves the bytes alone.</details>

e14. `[07-book]` In `OrderBook.add`, put the new level into the TreeMap *before* `level.addOrder(order)` (`src/main/java/exchangeserver/book/OrderBook.java:66-69`).
<details><summary>Answer</summary><strong>[Inferred]</strong> Probably no test fails: <code>addOrder</code> cannot throw on a fresh level today; the ordering is defence in depth.</details>

---

## Key takeaways

- Predict before you run: every answer follows from the paths in guide 05.
- Two sequences to keep apart: `seq` (journal, global) and `oseq` (per participant private stream).
- Some guarantees have no failing test (fsync, directory fsync, data races); some are covered only partly (CHECK every 10,000 commands).
- The live server plus `ClientMain` is the fastest feedback loop; `--check-interval` and `--compact` make the recovery features visible.

## Common confusions

- **"The duplicate's orderId=2 means no id was used."** Id 3 was used.
- **"A protocol ERROR or RATE_LIMITED is a reject."** Neither reaches the journal; a reject does.
- **"My reply went to the session that sent the order only."** It goes to every session of the participant (D73).
- **"If tests pass, concurrency is right."** No: happens-before arguments, not tests.

## Questions to test yourself

1. In b7, why does `WELCOME` say `oseq=4` though alice never saw event 4?
<details><summary>Answer</summary>The CANCEL_ALL ran after her last session closed; its cancel reply was numbered and retained in her stream. WELCOME reports the latest number; RESEND fetches what she missed.</details>

2. Which break-it experiments show a mistake no test catches?
<details><summary>Answer</summary>e4 and e12 (durability), e9 on Windows, e14, and e5 when no CHECK covers the changed command.</details>

3. In c1, why does `appendAll` return before `MatchingEngine.process` is entered?
<details><summary>Answer</summary>Write-ahead: the whole batch is appended and fsynced first.</details>
