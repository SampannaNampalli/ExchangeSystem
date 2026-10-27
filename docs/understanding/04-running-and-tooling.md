# 04 — Running and Tooling

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** install, every setting, build, run, test, mutation testing, benchmarks, debugging and "deployment", with the real output observed on 2026-10-27 | **Prerequisites:** 01, 03 | **Related:** 02, 05, 08, 10, 14
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

You need only a **JDK 17**. The Maven Wrapper downloads Maven itself, and the project has no runtime dependencies,
no database, no environment variables and no configuration files. Everything configurable is either one of three
command-line flags or a constant in the code (listed below).

Commands are shown for Windows (`mvnw.cmd`, the author's and owner's platform). On macOS/Linux use `./mvnw` and
`/` paths. All outputs marked **[Verified]** were observed on the owner's machine on 2026-10-27 (Windows 11,
Temurin 17.0.20.1).

## 1. Install

1. Install a JDK 17 (e.g. Eclipse Temurin 17) and make sure `java -version` prints 17. **[Verified]** observed:

   ```
   openjdk version "17.0.20.1" 2026-08-18
   OpenJDK Runtime Environment Temurin-17.0.20.1+1 (build 17.0.20.1+1)
   ```

2. Clone or open the repository. In IntelliJ: *File > Open > `pom.xml`* (**[Verified]** README).
3. The first `mvnw.cmd` call downloads Maven 3.9.16 and the dependencies (JUnit, JMH; PIT only with `-Pmutation`)
   into your user Maven cache; it needs internet access once.

## 2. Settings: everything that can be configured

### 2.1 Command-line flags (`Main`)

```java
// src/main/java/exchangeserver/Main.java:28-45 (trimmed)
int port = 9000;
Path journal = Path.of("exchange.journal");
Long quarantine = null;
for (int i = 0; i < args.length; i++) {
    switch (args[i]) {
        case "--port" -> port = Integer.parseInt(args[++i]);
        case "--journal" -> journal = Path.of(args[++i]);
        case "--quarantine" -> quarantine = Long.parseLong(args[++i]);
        default -> throw new IllegalArgumentException("unknown option " + args[i]);
    }
}
// any RuntimeException here: print "Usage: ..." and exit 1
```

| Flag | Default | Effect |
|---|---|---|
| `--port N` | `9000` | TCP port to listen on, **on all network interfaces** (**[Verified]** `src/main/java/exchangeserver/gateway/TcpGateway.java:67`). `0` picks a free port (used by tests). |
| `--journal FILE` | `exchange.journal` in the current directory | The journal file. Created with a header if missing or empty. Must be a journal written under the same `Rules.VERSION`, otherwise startup fails. |
| `--quarantine SEQ` | none | Do not start the server: append `SKIP,<next seq>,<now>,SEQ` to the journal and exit. Refused if no NEW/CANCEL record has that sequence number, or if the journal is open in a running exchange (D65). |

**[Verified]** `src/main/java/exchangeserver/Main.java:28-58`. There are no environment variables and no config
files (**[Verified]** by search: no `System.getenv`, no `Properties` loading in `src/main`).

### 2.2 Constants in the code

| Setting | Value | Where | Changeable without editing code? |
|---|---|---|---|
| Queue capacity (requests waiting for the writer) | 100,000 | `src/main/java/exchangeserver/core/ExchangeService.java:69` (`Settings.defaults()`) | No (only via the Java API `ExchangeService.start(..., settings)`) |
| Max batch size | 1,000 | same line | No |
| Linger (group-commit wait) | 0 = off (smart batching) | same line | No; kept only for benchmarks (Spec D52) |
| Clock | `Clock.systemUTC()` | same line | No |
| Max sessions | 256 | `src/main/java/exchangeserver/gateway/TcpGateway.java:59` | No |
| Outbound queue per session | 10,000 lines | same line | No |
| Max line length (excluding `\r\n`) | 256 bytes | `src/main/java/exchangeserver/gateway/Protocol.java:44` | No |
| Snapshot depth on SUBSCRIBE | 100 levels per side | `src/main/java/exchangeserver/gateway/Protocol.java:48` | No |
| Accept back-off after a failed `accept()` | 100 ms | `src/main/java/exchangeserver/gateway/TcpGateway.java:46` | No |
| Shutdown wait for the writer | 10 s | `src/main/java/exchangeserver/Main.java:77` | No |
| `Rules.VERSION` | 1 | `src/main/java/exchangeserver/domain/Rules.java:19` | No, and changing any rule below requires bumping it |
| Max order quantity | 1,000,000,000 | `src/main/java/exchangeserver/domain/Rules.java:22` | No |
| Max price | `Long.MAX_VALUE / 1e9` ≈ 9,223,372,036 | `src/main/java/exchangeserver/domain/Rules.java:24` | No |
| Participant name | `[A-Za-z0-9_]{1,16}` | `src/main/java/exchangeserver/domain/Rules.java:26` | No |
| Client order id | `[A-Za-z0-9_.-]{1,32}` | `src/main/java/exchangeserver/domain/Rules.java:27` | No |
| Price band | ±10% of the last trade | `src/main/java/exchangeserver/domain/Rules.java:32` | No |
| Open orders per participant | 10,000 | `src/main/java/exchangeserver/domain/Rules.java:33` | No |

All **[Verified]** at the cited lines. **[Inferred]** The design treats rule values as code on purpose: they are
part of what the journal's rules version certifies (Spec D20, D46), so they must not drift per deployment.

## 3. Build

```
mvnw.cmd package                    compile, run all tests, build the jar
mvnw.cmd package -DskipTests        build the jar without tests (what the owner used for live runs)
mvnw.cmd clean package -DskipTests  same, from scratch
```

Output: `target/exchange-server2-2.0.0-SNAPSHOT.jar` (~110 KB, **[Verified]**). The jar's manifest names
`exchangeserver.Main` (**[Verified]** `pom.xml:78-89`), and the jar also contains the client classes.

The compiler runs with `-Xlint:all` (minus annotation-processing notes); the project rule is "keep the build
warning-free" (**[Verified]** `pom.xml:62-65`, `CLAUDE.md`).

**Stale-build trap (Windows and elsewhere).** Maven's compiler plugin recompiles a source only if it is newer than
its `.class`. If you restore an older copy of a file (for example with PowerShell's `Copy-Item`, which keeps the old
modification time), the jar keeps the *newer* compiled code and nothing warns you. **[Verified]**: this happened on
2026-10-27 and made a live test exercise old code. Use `mvnw.cmd clean ...` after swapping files.

**Do not run two Maven builds at once**: they share `target/` (**[Verified]** `CLAUDE.md`).

## 4. Run the server and a client

### 4.1 Start the server

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal exchange.journal
```

**[Verified]** output (one line on stdout, then nothing until shutdown):

```
ExchangeServer2 listening on port 9000, journal C:\...\exchange.journal
```

Startup took about 0.5 s with a tiny journal (**[Verified]**, measured at 472 ms). Startup time grows with the
journal size, because the whole journal is replayed twice (verify, then feed) and there are no snapshots (Spec §8).

**Files that appear next to the journal.** Besides `exchange.journal`, the first start creates an **empty
`exchange.journal.lock`** in the same directory (**[Verified]** `src/main/java/exchangeserver/journal/CommandJournal.java:209-215`).
While the exchange runs, it holds an exclusive OS lock on that file, so a second exchange or a `--quarantine` run
on the same journal is refused with "already open" (D65). The lock file is **deliberately left on disk** after a
stop (deleting it could race with the next opener, `src/main/java/exchangeserver/journal/CommandJournal.java:205`);
it is safe to leave, safe to delete while nothing is running, ignored by git (`*.journal.lock`), and holds no data.
The lock, not the file's existence, is what matters: a leftover lock file from a killed process does not block the
next start, because the OS releases the lock when the process dies (**[Verified]** on Windows during the survey: the
server was hard-killed, `live2.journal.lock` (0 bytes) stayed on disk, and the restart on the same journal came up
and served `BOOK seq=1 last=0 bids=- asks=100x3`; on Linux/macOS **[Inferred]** from standard OS lock behaviour).

### 4.2 Connect a client

Interactive console client:

```
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000
```

It prints `Connected to localhost:9000. Try: LOGIN alice / NEW c1 BUY LIMIT 10 100 / SUBSCRIBE / QUIT`, then echoes
each server line prefixed with `< ` (**[Verified]** `src/main/java/exchangeserver/client/ClientMain.java:39`). Any
raw TCP tool also works (telnet, `nc`, PowerShell's `System.Net.Sockets.TcpClient`): the protocol is plain ASCII
lines ending in `\n` (`\r\n` accepted).

The protocol in one screen (**[Verified]** `src/main/java/exchangeserver/gateway/Protocol.java:17-38`):

```
Client -> server
    LOGIN <participant>
    NEW <clOrdId> <BUY|SELL> <LIMIT|IOC|FOK|POST_ONLY> <quantity> <price>
    NEW <clOrdId> <BUY|SELL> MARKET <quantity>
    CANCEL <orderId>
    SUBSCRIBE | UNSUBSCRIBE | PING | QUIT
Server -> client
    WELCOME participant=<p>
    FILL orderId= clOrdId= side= price= qty= leaves= liquidity=TAKER|MAKER
    ORDER orderId= clOrdId= status= filled= remaining= [reason=]
    CANCELLED orderId= qty=          CANCEL_REJECT orderId= reason=UNKNOWN_ORDER
    BOOK seq= last= bids=<price>x<qty>,... asks=...
    TRADE seq= price= qty= aggressor=          LEVEL seq= side= price= qty= orders=
    SUBSCRIBED  UNSUBSCRIBED  PONG  BYE
    ERROR <text>
QUIT: every request sent before it is answered first, then BYE, then the server closes the connection.
```

### 4.3 A real session (two clients)

**[Verified]** observed 2026-10-27 against a fresh journal, driven by a script with two TCP connections (`>` is
what a client sent, `<` what it received):

```
alice > NEW x1 BUY LIMIT 1 100
alice < ERROR LOGIN first
alice > LOGIN alice
alice < WELCOME participant=alice
bob > LOGIN bob
bob < WELCOME participant=bob
alice > NEW a1 SELL LIMIT 10 100
alice < ORDER orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10
alice > SUBSCRIBE
alice < SUBSCRIBED
alice < BOOK seq=1 last=0 bids=- asks=100x10
bob > NEW b1 BUY LIMIT 4 101
bob < FILL orderId=2 clOrdId=b1 side=BUY price=100 qty=4 leaves=0 liquidity=TAKER
bob < ORDER orderId=2 clOrdId=b1 status=FILLED filled=4 remaining=0
alice < FILL orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER
alice < TRADE seq=2 price=100 qty=4 aggressor=BUY
alice < LEVEL seq=2 side=SELL price=100 qty=6 orders=1
bob > NEW b1 BUY LIMIT 4 101
bob < ORDER orderId=2 clOrdId=b1 status=REJECTED filled=0 remaining=4 reason=DUPLICATE_CLIENT_ORDER_ID
bob > NEW b2 BUY LIMIT 1 200
bob < ORDER orderId=4 clOrdId=b2 status=REJECTED filled=0 remaining=1 reason=PRICE_OUTSIDE_BAND
bob > NEW b3 BUY MARKET 1 100
bob < ERROR NEW takes 4 field(s), got 5
bob > CANCEL 1
bob < CANCEL_REJECT orderId=1 reason=UNKNOWN_ORDER
alice > CANCEL 1
alice < CANCELLED orderId=1 qty=6
alice < LEVEL seq=6 side=SELL price=100 qty=0 orders=0
bob > PING
bob < PONG
alice > QUIT
alice < BYE
bob > QUIT
bob < BYE
```

Things worth noticing (each is explained in guides 05 and 07):

- The duplicate `b1` is rejected **with the original order id 2**, so a client that lost a reply can learn what
  happened (Spec D49). The duplicate still used up order id 3, which is why `b2` gets id 4.
- `b2` at 200 is more than 10% above the last trade (100), so `PRICE_OUTSIDE_BAND`.
- `NEW b3 BUY MARKET 1 100` is a *syntax* error (a market order has no price field) and never reaches the
  exchange, so it uses no sequence number.
- Bob's `CANCEL 1` targets Alice's order: it looks unknown (`UNKNOWN_ORDER`), never "not yours" (Spec D48).
- The market-data sequence numbers jump from 2 to 6: sequence numbers count every journalled command, and only
  some produce market data.

The journal after that session (**[Verified]**, the file is plain text):

```
HEADER,2,1
NEW,1,2026-10-27T12:38:33.890909300Z,1,alice,a1,SELL,LIMIT,10,100
NEW,2,2026-10-27T12:38:34.862205500Z,2,bob,b1,BUY,LIMIT,4,101
NEW,3,2026-10-27T12:38:35.732225200Z,3,bob,b1,BUY,LIMIT,4,101
NEW,4,2026-10-27T12:38:36.154630600Z,4,bob,b2,BUY,LIMIT,1,200
CANCEL,5,2026-10-27T12:38:36.991832100Z,bob,1
CANCEL,6,2026-10-27T12:38:37.410921700Z,alice,1
```

The two rejected NEWs (records 3 and 4) are journalled because their rejection depended on state; the
unauthorised cancel (5) too. The `ERROR` lines are not.

### 4.4 Restart and crash recovery

**[Verified]** observed: the server above was killed hard (no shutdown hook ran), half a record
(`NEW,7,2026-10-27T12:`) was appended to simulate a crash mid-write, and the server restarted on the same journal:

```
> LOGIN carol
< WELCOME participant=carol
> SUBSCRIBE
< SUBSCRIBED
< BOOK seq=6 last=100 bids=- asks=-
> NEW c1 BUY LIMIT 5 99
< ORDER orderId=5 clOrdId=c1 status=RESTED filled=0 remaining=5
< LEVEL seq=7 side=BUY price=99 qty=5 orders=1
```

The torn record was cut silently (nothing is printed about it), the book and the last trade price came back, and
the counters continued at sequence 7 and order id 5. The new record replaced the torn bytes:
`NEW,7,2026-10-27T12:38:57.279532800Z,5,carol,c1,BUY,LIMIT,5,99`.

### 4.5 Startup failures (exit code 1)

**[Verified]** observed, journal whose header says rules version 9:

```
Startup failed: java.lang.IllegalStateException: Journal C:\...\v9.journal was written under rules version 9; this build uses 1. Start a fresh journal from a snapshot.
If replay failed on one command (a poison pill), inspect it, then restart with --quarantine <seq>.
```

(The second line is printed for every startup failure, even when it does not apply; and "from a snapshot" refers to
a feature that does not exist yet, Spec §8.)

**[Verified]** observed, a file that is not a journal (`--journal notes.txt` containing `keep me\nlast line`):

```
Startup failed: java.lang.IllegalStateException: Bad journal header "keep me": first record is not a header
```

and the file is left unchanged (D64).

**[Verified]** observed, a journal converted to CRLF line endings (e.g. by an editor or PowerShell `Set-Content`):

```
Startup failed: java.lang.IllegalStateException: Bad journal header "HEADER,2,9\r": For input string: "9\r"
```

Records are LF-only; a `\r` becomes part of the last field (Spec §8). **[Inferred]** A UTF-8 byte-order mark at the
start of the file would fail the same way (`first record is not a header`). Edit journals only with tools that keep
LF and no BOM, and only after a backup.

### 4.6 Quarantining a poison pill

If startup fails while replaying one command (an engine exception on record *k*), the message names the record.
After inspecting it:

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal exchange.journal --quarantine 7
```

Output on success: `Quarantined command 7 in C:\...\exchange.journal` (**[Verified]**, observed). The journal gains
`SKIP,<next seq>,<now>,7`; the next startup skips record 7.

While a server has the journal open, the tool is refused (D65): it cannot take the lock on
`exchange.journal.lock`. Stop the exchange first. Expected output
(**[Verified]** from `src/main/java/exchangeserver/Main.java:50-54` and
`src/main/java/exchangeserver/journal/CommandJournal.java:249-251`; a cross-process refusal is also covered by the test
`CommandJournalTest.Exclusive.anotherProcessIsRefusedWhileTheJournalIsOpen`, which runs `Main --quarantine` in a
child JVM):

```
Quarantine failed: Journal C:\...\exchange.journal is already open (a running exchange, or a --quarantine run)
```

### 4.7 Stopping, and exit codes

| How it ends | Exit code | What happens |
|---|---|---|
| Ctrl+C / SIGTERM | 0 per `Main`'s comment; **unverified**, likely 130/143 on Linux/macOS (signal exit; guide 15 #24) | Shutdown hook: close the gateway (drop connections), `stop()` the service (refuse new requests, process everything already accepted, close the journal), wait up to 10 s (**[Verified]** `src/main/java/exchangeserver/Main.java:74-83`). **Not exercised** in the survey. |
| Halt (journal I/O failure, `Error` in the engine, writer interrupted) | 2 | `Exchange halted: <cause>` on stderr, gateway closed, `System.exit(2)` (**[Verified]** `src/main/java/exchangeserver/Main.java:85-90`) |
| Bad arguments, startup failure, failed quarantine | 1 | One-line message on stderr |
| Hard kill (`kill -9`, Task Manager) | n/a | Nothing runs; the journal is the truth; restart replays it (observed above) |

## 5. Tests

```
mvnw.cmd test                                           all tests
mvnw.cmd test -Dtest=MatchingEngineTest                 one class, including its @Nested groups
mvnw.cmd test -Dtest=RulesTest#oppositeSide             one test method
mvnw.cmd test "-Dtest=ExchangeServiceTest,TcpGatewayTest"   several classes
```

**[Verified]** commands from `CLAUDE.md`; the multi-class form was used during the survey. In PowerShell, quote
`-D` arguments containing commas, `#` or `$`. **[Inferred]** a single nested group can be selected as
`"-Dtest=ExchangeServiceTest`$Batching"` (Surefire treats nested classes as `Outer$Inner`); not tried.

Expected end of output (**[Verified]**, after the 2026-10-27 fixes):

```
[INFO] Tests run: 334, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

The full run takes about a minute or two on the dev laptop (**[Inferred]** from the session's timings).

**Expected noise** (not failures, **[Verified]** `CLAUDE.md` and observed): tests that inject poison pills or fatal
errors log stack traces, e.g.

```
SEVERE: Exchange halted
java.lang.OutOfMemoryError: simulated
	at exchangeserver.core.ExchangeServiceTest$Failures.lambda$fatalErrorHaltsAndEveryoneWaitingIsTold$1(...)
```

and `simulated engine bug`. Under PowerShell, anything a native program writes to stderr is shown as
`NativeCommandError`; that is PowerShell's wrapping, not a failure.

**Reports:** `target/surefire-reports/` (one `.txt` and `.xml` per test class). Only the last run's reports are
kept, so to study an intermittent failure save each run's console output.

**History:** the first full run of the survey had 1 failure out of 326
(`ExchangeServiceTest$Batching.waitingRequestsShareOneBatch: expected: <50> but was: <1>`), which then passed 8 of 8
runs of that class alone: a timing race in the test, fixed by reading the writer's stats after `stop()`
(**[Verified]** Spec §10).

## 6. Mutation testing (PIT)

```
mvnw.cmd -Pmutation test
```

Runs the normal tests, then PIT over `domain`, `book`, `engine`, `command`, `journal`, `core` with 4 threads; takes
several minutes. Report: `target/pit-reports/index.html` (**[Verified]** `pom.xml:127-169`, README). The project
rule: after a behavioural change, run it and explain survivors in Spec §7; fsync (`force`) and log calls are expected
survivors because no unit test can observe them (**[Verified]** `CLAUDE.md`, Spec §7).

## 7. Benchmarks (JMH)

```
mvnw.cmd -Pbench verify -Djmh.args="MatchingBenchmark -f 1 -wi 3 -w 1s -i 5 -r 1s"
mvnw.cmd -Pbench verify -Djmh.args="BatchingBenchmark -f 1 -wi 2 -w 2s -i 3 -r 3s -t 16"
```

`-f` forks, `-wi`/`-w` warm-up iterations/time, `-i`/`-r` measured iterations/time, `-t` threads (= simulated
clients for `BatchingBenchmark`). The `bench` profile skips tests. `BatchingBenchmark` writes real journals under
`target/bench/` because the cost of fsync is the point (**[Verified]**
`src/test/java/exchangeserver/bench/BatchingBenchmark.java:25-39`). **Not run** during the survey; recorded results
are in Spec §5 (e.g. a command costs ~0.65–1.1 µs; smart batching reaches 43 orders/ms with 64 clients).

## 8. Debugging

### 8.1 Threads you will see (names are set in code)

| Thread | Daemon? | Role | Where |
|---|---|---|---|
| `main` | no | Runs `Main`, then waits in `awaitTermination()` | `src/main/java/exchangeserver/Main.java:85` |
| `exchange-writer` | **no** | The single writer: batches, journal, engine, replies, listener | `src/main/java/exchangeserver/core/ExchangeService.java:112-114` |
| `gateway-acceptor` | yes | `accept()` loop | `src/main/java/exchangeserver/gateway/TcpGateway.java:79-80` |
| `session-N-reader` | yes | Parses lines from client N, hands in requests | `src/main/java/exchangeserver/gateway/ClientSession.java:62-64` |
| `session-N-sender` | yes | Writes client N's outbound queue to its socket | `src/main/java/exchangeserver/gateway/ClientSession.java:63-65` |
| `ForkJoinPool.commonPool-worker-*` | yes | Completes futures handed to API callers (D61) | `src/main/java/exchangeserver/core/ExchangeService.java:154-164` |
| `shutdown` | no | The shutdown hook | `src/main/java/exchangeserver/Main.java:74-83` |

A thread dump (`jstack <pid>` or `jcmd <pid> Thread.print`) shows instantly where each is blocked. An idle exchange
has `exchange-writer` waiting in `LinkedBlockingQueue.take`, readers in `SocketInputStream.read`, senders in
`ArrayBlockingQueue.take`. **[Inferred]** from the code paths; not captured during the survey.

### 8.2 Good breakpoints

| To see… | Break at |
|---|---|
| A batch being stamped and journalled | `src/main/java/exchangeserver/core/Exchange.java:118` (`process`) and `:137` (`journal.appendAll`) |
| One order being matched | `src/main/java/exchangeserver/engine/MatchingEngine.java:106` (`process`), the loop at `:137` |
| What a client line becomes | `src/main/java/exchangeserver/gateway/ClientSession.java:182` (`handle`) |
| Lines leaving for a client | `ClientSession.send` at `src/main/java/exchangeserver/gateway/ClientSession.java:78` |
| Replay at startup | `src/main/java/exchangeserver/core/Replay.java:36` (`run`) |
| A poison pill | `src/main/java/exchangeserver/core/Exchange.java:178` (`quarantine`) |

Caution: **a breakpoint on the writer thread freezes the whole exchange** (by design nothing else touches the
book), and if it sits long enough every client's requests pile up in the queue until BUSY. Prefer "suspend thread"
over "suspend all", and prefer tests with the `CommandInterceptor` seam for reproducible pauses (guide 10).

To debug a running jar from an IDE, start it with the standard JDWP agent (**[General knowledge]**):

```
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005 -jar target/exchange-server2-2.0.0-SNAPSHOT.jar
```

### 8.3 Logs

Only three classes log, through `System.Logger`, which (with no logging library) goes to `java.util.logging` and
prints to **stderr** (**[Verified]** guide 02 §6):

| Message | Level | Meaning | Source |
|---|---|---|---|
| `Engine failed on command N; quarantining it` + stack trace | ERROR (shown as `SEVERE`) | A poison pill was quarantined live | `src/main/java/exchangeserver/core/Exchange.java:179` |
| `Output listener failed on command N` | ERROR | The gateway's listener threw; ignored | `src/main/java/exchangeserver/core/Exchange.java:163` |
| `Exchange halted` + cause | ERROR | Writer halted; process will exit 2 | `ExchangeService.halt` (search `Exchange halted` in `src/main/java/exchangeserver/core/ExchangeService.java`) |
| `Closing the journal failed` | WARNING | | `src/main/java/exchangeserver/core/ExchangeService.java:275` |
| `accept failed; retrying` | WARNING | Transient accept error (e.g. too many open files); retried every 100 ms | `src/main/java/exchangeserver/gateway/TcpGateway.java:99` |
| `session N is too slow; disconnecting` | INFO | A client's outbound queue filled | `src/main/java/exchangeserver/gateway/ClientSession.java:83` |

There is no request log, no metrics and no log of startup facts (commands replayed, torn bytes cut). To change log
levels, pass a standard `java.util.logging` config file with `-Djava.util.logging.config.file=...`
(**[General knowledge]**).

### 8.4 Inspecting state

- **The journal is the best debugging tool**: plain text, one command per line, in processing order. Replaying it
  in a test (`Exchange.open(path, clock)` then `snapshot(depth)`) rebuilds the exact book.
- **The live book**: `SUBSCRIBE` from any client gives a `BOOK` snapshot (best 100 levels per side).
- From code (tests): go through the queue (`service.snapshot(depth)`), never read the `OrderBook` from another
  thread (**[Verified]** `CLAUDE.md`, "Single writer").

## 9. Windows-specific notes

**[Verified]** from `CLAUDE.md` and Spec §1.5 unless marked:

- The default charset is windows-1252; the code always passes a charset, and the build sets UTF-8. Do the same in
  any code you add.
- **Never write Java sources with Windows PowerShell's `Set-Content -Encoding utf8`**: it adds a byte-order mark,
  which `javac` rejects. Use an editor, or `[IO.File]::WriteAllText(path, text, New-Object Text.UTF8Encoding($false))`.
- PowerShell `Set-Content`/`Out-File` also write CRLF line endings: fatal for a journal (§4.5; observed).
- Java opens files with delete sharing on Windows, so deleting a file does not prove it was closed; tests use
  `CommandJournal.openJournals()` instead.
- File locks differ by OS: on Windows they are mandatory and belong to a handle; on Linux/macOS they are advisory,
  belong to the process, and are dropped when *any* handle on the file closes. That is why the exchange locks a
  separate `<journal>.lock` file that nothing else opens, not the journal itself (D65; guide 02 §3.2). The
  cross-process test has run on Windows only.
- One fsync costs ~1.5 ms on the dev disk; timed waits round up to the ~15.6 ms Windows timer (why the linger
  batching mode measured badly, Spec §5).
- The stale-build trap (§3).

## 10. Deployment

There is none: no container, no service definition, no CI configuration (**[Verified]** no such files in the
repo). The deliverable is the jar plus a journal file. **[Inferred]** a real deployment would need at least: a
process supervisor that restarts on exit code 2 (but not blindly on exit 1, which needs an operator); the journal on
a durable local disk with a backup policy; the port firewalled, since the server binds all interfaces and `LOGIN`
has no authentication (guides 08 and 11).

## Key takeaways

- Only JDK 17 is needed; the wrapper fetches Maven; no env vars or config files.
- Three flags: `--port` (9000), `--journal` (`exchange.journal`), `--quarantine <seq>`; everything else is a code
  constant, and rule constants are certified by `Rules.VERSION`.
- `mvnw.cmd package -DskipTests` then `java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar`; connect with
  `ClientMain` or any line-based TCP tool.
- Exit codes: 1 startup/argument/quarantine failure, 2 halted; a clean stop is documented as 0, but after Ctrl+C the
  JVM's signal exit status may apply instead (unverified, guide 15 #24).
- Tests: `mvnw.cmd test` → 334 green; stack traces from simulated failures are expected noise.
- `-Pmutation test` for PIT, `-Pbench verify -Djmh.args=...` for JMH.
- Debug by thread name; a breakpoint on `exchange-writer` freezes the exchange; the plain-text journal is the best
  state inspector.
- Windows traps: CRLF/BOM in journals and sources, stale incremental builds, coarse timers.
- An empty `<journal>.lock` file sits next to each journal; it is the "in use" lock (D65), safe to leave.

## Common confusions

- **"I changed `queueCapacity` in a config file."** There is none. Settings are `ExchangeService.Settings`, set
  only through the Java API; `Main` always uses `Settings.defaults()`.
- **"Exit code 1 means the exchange crashed."** No: 1 means it never started (or the quarantine tool refused).
  A running exchange that fails halts with exit code 2.
- **"The tool printed `Quarantined command 7`, so the poison is gone."** The poison record stays in the journal
  (history is annotated, never edited); a `SKIP` record after it tells replay to pass over it.
- **"A leftover `exchange.journal.lock` means the exchange is still running."** No: the file always stays. Only a
  live OS lock on it means "in use"; if nothing is running, it is just an empty file.
- **"The torn tail cut would be logged."** It is not; `Exchange.truncatedBytesOnOpen()` exists but `Main` does not
  print it.
- **"`-DskipTests` skips compiling tests."** It skips running them; test sources (and the JMH processor) still
  compile. **[General knowledge]** (`-Dmaven.test.skip=true` skips both.)

## Questions to test yourself

1. You start the jar twice on the same journal with different ports. What happens to the second?
   <details><summary>Answer</summary>It binds its port, then `CommandJournal.open` fails to take the lock on
   `exchange.journal.lock` and throws `IllegalStateException("... is already open ...")`; `Main` prints
   `Startup failed: ...` and exits 1 (D65).</details>
2. In the transcript, why does `b2` get order id 4 although only three orders were accepted before it?
   <details><summary>Answer</summary>Order ids are issued by the sequencer to every journalled new order, including
   the rejected duplicate `b1` (id 3). Rejected state-dependent orders are still journalled and use up an id.</details>
3. Which lines of the transcript produced no journal record, and why?
   <details><summary>Answer</summary>`LOGIN`, `SUBSCRIBE`, `PING`, `QUIT` (session-level, not exchange commands),
   the `NEW x1` before login, and `NEW b3 BUY MARKET 1 100` (syntax error): stateless refusals and session messages
   never reach the queue.</details>
4. A colleague edits the journal in Notepad to remove a bad record, and the server no longer starts with
   `Bad journal header "HEADER,2,1\r"`. What happened and what should they have done?
   <details><summary>Answer</summary>The editor saved CRLF line endings, so every record now ends with `\r`, which
   the codec keeps as part of the last field. They should never edit history; use `--quarantine <seq>` to skip a
   record, and if a file must be repaired, keep LF and no BOM.</details>
5. How do you make `MatchingEngineTest` stop at the start of every order match in your IDE without freezing any
   real clients?
   <details><summary>Answer</summary>Run the test in debug mode with a breakpoint at
   `MatchingEngine.process` (`src/main/java/exchangeserver/engine/MatchingEngine.java:106`); the engine tests call
   the engine directly on the test thread, with no writer thread or sockets involved.</details>
6. Why does the first line of a server's stderr during `mvnw.cmd test` say `NativeCommandError` in PowerShell?
   <details><summary>Answer</summary>PowerShell wraps anything a native program writes to stderr as an error record;
   the tests' expected log output (simulated failures) goes to stderr. It is not a test failure.</details>
