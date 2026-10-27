# 04 — Running and Tooling

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** install, every setting, users and TLS, build, run, operator tools, tests (Windows and Linux), mutation testing, benchmarks, debugging and "deployment", with the real output observed on 2026-10-27 | **Prerequisites:** 01, 03 | **Related:** 02, 05, 08, 10, 14
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

You need only a **JDK 17**. The Maven Wrapper downloads Maven itself, and the project has no runtime dependencies
and no database. What you configure:

- **command-line flags** of `Main` (port, bind address, journal, users file, TLS keystore, timeouts, limits, and
  three operator tools);
- **a users file** of participants and hashed secrets, written with `--add-user`;
- **two environment variables**, only for TLS passwords (`ES2_TLS_PASSWORD`, `ES2_TRUSTSTORE_PASSWORD`);
- **constants in the code** for everything else (listed below).

Commands are shown for Windows (`mvnw.cmd`, the author's and owner's platform). On macOS/Linux use `./mvnw` and
`/` paths. Outputs marked **[Verified — observed]** were seen on 2026-10-27, on Windows 11 with Temurin 17.0.20.1,
or in a Linux container (`eclipse-temurin:17`).

## 1. Install

1. Install a JDK 17 (e.g. Eclipse Temurin 17) and make sure `java -version` prints 17. **[Verified — observed]**:

   ```
   openjdk version "17.0.20.1" 2026-08-18
   OpenJDK Runtime Environment Temurin-17.0.20.1+1 (build 17.0.20.1+1)
   ```

2. Clone or open the repository. In IntelliJ: *File > Open > `pom.xml`* (**[Verified]** README).
3. The first `mvnw.cmd` call downloads Maven 3.9.16 and the dependencies (JUnit, JMH; PIT only with `-Pmutation`)
   into your user Maven cache; it needs internet access once.

## 2. Settings: everything that can be configured

### 2.1 Command-line flags (`Main`)

`Main` documents its own usage at the top of the file (**[Verified]** `src/main/java/exchangeserver/Main.java:25-40`):

```
java -jar exchange-server2.jar [--port 9000] [--bind 127.0.0.1] [--journal exchange.journal]
                               [--users exchange.users] [--tls-keystore FILE]
                               [--login-timeout 10] [--idle-timeout 300] [--max-in-flight 1000]
                               [--rate 1000] [--burst 2000] [--check-interval 10000] [--resend-window 1000]
java -jar exchange-server2.jar --users exchange.users --add-user <participant>    (asks for the secret)
java -jar exchange-server2.jar --journal exchange.journal --quarantine <seq>
java -jar exchange-server2.jar --journal exchange.journal --compact
```

| Flag | Default | Effect |
|---|---|---|
| `--port N` | `9000` | TCP port. `0` picks a free port (used by tests). |
| `--bind ADDR` | `127.0.0.1` (loopback) | Address to listen on. Anything not loopback reaches the network: use it with TLS; without TLS `Main` prints a warning that secrets cross the network in clear text (**[Verified]** `src/main/java/exchangeserver/Main.java:127-129`, Spec D74). |
| `--journal FILE` | `exchange.journal` in the current directory | The journal. Created with a header if missing or empty. Must have been written under the same `Rules.VERSION` (2), unless it has no records yet (then it adopts this build's version, D71). |
| `--users FILE` | `exchange.users` in the current directory | The users file (participant + salted PBKDF2 hash). **Required**: the server does not start without it. |
| `--tls-keystore FILE` | none (plain TCP) | A PKCS#12 keystore; enables TLS 1.3/1.2. Password from the `ES2_TLS_PASSWORD` environment variable. |
| `--login-timeout S` | `10` | Seconds a new connection has to complete `LOGIN`. |
| `--idle-timeout S` | `300` | Seconds a logged-in session may stay silent (`PING` keeps it alive). |
| `--max-in-flight N` | `1000` | Requests a session may have handed in and not yet completed (`SESSION_BUSY` beyond). |
| `--rate N`, `--burst N` | `1000`, `2000` | Token bucket per session: requests per second, and the burst size (`RATE_LIMITED` beyond). |
| `--check-interval N` | `10000` | Commands between `CHECK` records (output digest, D70). |
| `--resend-window N` | `1000` | Private events kept per participant for `RESEND` (D73). |
| `--add-user P` | — | Tool: ask for P's secret, write its hash to the users file, exit. |
| `--quarantine SEQ` | — | Tool: append `SKIP,<next seq>,<now>,SEQ` to the journal and exit. Refused if no NEW/CANCEL/CANCEL_ALL record has that sequence number, if it is already quarantined, or if the journal is open elsewhere. |
| `--compact` | — | Tool: snapshot + archive + empty journal (D71), exit. Refused if the journal is open elsewhere. |

**[Verified]** `src/main/java/exchangeserver/Main.java:48-117`. Any parse error prints
`Usage: see the comment at the top of Main.java (...)` and exits 1.

### 2.2 Environment variables

| Variable | Used by | When |
|---|---|---|
| `ES2_TLS_PASSWORD` | `Main` (server) | Only with `--tls-keystore` |
| `ES2_TRUSTSTORE_PASSWORD` | `ClientMain` | Only with `--tls --truststore FILE` |

**[Verified]** `src/main/java/exchangeserver/Main.java:126`, `src/main/java/exchangeserver/client/ClientMain.java:12-20`,
`src/main/java/exchangeserver/gateway/Tls.java:52-58`. Why not flags: command-line arguments are visible to other
users in the process list (`src/main/java/exchangeserver/gateway/Tls.java:15-17`). A missing variable fails with
`Set the ES2_TLS_PASSWORD environment variable to the keystore password`.

### 2.3 Constants in the code

| Setting | Value | Where |
|---|---|---|
| Queue capacity (requests waiting for the writer) | 100,000 | `src/main/java/exchangeserver/core/ExchangeService.java:70` (`Settings.defaults()`) |
| Max batch size | 1,000 | same line |
| Linger (group-commit wait) | 0 = off (smart batching; kept only for benchmarks, D52) | same line |
| Clock | `Clock.systemUTC()` | same line |
| Max sessions | 256 | `src/main/java/exchangeserver/gateway/TcpGateway.java:69` |
| Outbound queue per session | 10,000 lines | same line |
| Max line length (excluding `\r\n`) | 256 bytes | `src/main/java/exchangeserver/gateway/Protocol.java:50` |
| Snapshot depth on SUBSCRIBE | 100 levels per side | `src/main/java/exchangeserver/gateway/Protocol.java:54` |
| Accept back-off after a failed `accept()` | 100 ms doubling to 5 s; ERROR log after 10 failures in a row | `src/main/java/exchangeserver/gateway/TcpGateway.java:100-102` |
| PBKDF2 iterations for `--add-user` | 210,000 | `src/main/java/exchangeserver/gateway/Credentials.java:36` |
| Secret format | 8–64 printable ASCII characters, no spaces | `src/main/java/exchangeserver/gateway/Credentials.java:38` |
| Shutdown wait for the writer | 10 s | `src/main/java/exchangeserver/Main.java:163` |
| `Rules.VERSION` | 2 | `src/main/java/exchangeserver/domain/Rules.java:21` |
| Max order quantity | 1,000,000,000 | `src/main/java/exchangeserver/domain/Rules.java:24` |
| Max price | `Long.MAX_VALUE / 1e9` ≈ 9,223,372,036 | `src/main/java/exchangeserver/domain/Rules.java:26` |
| Participant name | `[A-Za-z0-9_]{1,16}` | `src/main/java/exchangeserver/domain/Rules.java:28` |
| Client order id | `[A-Za-z0-9_.-]{1,32}` | `src/main/java/exchangeserver/domain/Rules.java:29` |
| Price band | ±10% of the last trade | `src/main/java/exchangeserver/domain/Rules.java:34` |
| Resting orders per participant | 10,000 | `src/main/java/exchangeserver/domain/Rules.java:35` |
| Resting orders in total | 1,000,000 | `src/main/java/exchangeserver/domain/Rules.java:37` |

All **[Verified]** at the cited lines. **[Inferred]** The design treats rule values as code on purpose: they are part
of what the journal's rules version certifies (Spec D20, D46), so they must not drift per deployment. Changing any
`Rules` value requires bumping `Rules.VERSION`.

## 3. Build

```
mvnw.cmd package                    compile, run all tests, build the jar
mvnw.cmd package -DskipTests        build the jar without tests (what the live runs used)
mvnw.cmd clean package -DskipTests  same, from scratch
```

Output: `target/exchange-server2-2.0.0-SNAPSHOT.jar`. The jar's manifest names `exchangeserver.Main`
(**[Verified]** `pom.xml:78-89`), and the jar also contains the client classes.

The compiler runs with `-Xlint:all` (minus annotation-processing notes); the project rule is "keep the build
warning-free" (**[Verified]** `pom.xml:62-65`, `CLAUDE.md`).

**Stale-build trap.** Maven's compiler plugin recompiles a source only if it is newer than its `.class`. If you
restore an older copy of a file (for example with PowerShell's `Copy-Item`, which keeps the old modification time),
the jar keeps the *newer* compiled code and nothing warns you. **[Verified]**: this happened on 2026-10-27 and made a
live test exercise old code. Use `mvnw.cmd clean ...` after swapping files.

**Do not run two Maven builds at once**: they share `target/` (**[Verified]** `CLAUDE.md`).

## 4. Run the server and a client

### 4.1 Create users

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --users exchange.users --add-user alice
```

In a terminal it asks `Secret for alice:` and `Again:` without echo; without a terminal (a script) it reads one
line from standard input (**[Verified]** `src/main/java/exchangeserver/Main.java:212-232`). **[Verified — observed]**
from PowerShell:

```
PS> "alice-secret" | java -jar target\exchange-server2-2.0.0-SNAPSHOT.jar --users exchange.users --add-user alice
Saved alice in C:\...\exchange.users
```

and the file holds one line per participant, no secret (**[Verified — observed]**, truncated):

```
alice:pbkdf2-sha256:210000:gJDXXBPuB1OYmFalrdCSx...
bob:pbkdf2-sha256:210000:Z/guAtluKSh/bT+M9ErbwA=...
```

A trap fixed on 2026-10-27: Windows PowerShell pipes text to a program with a UTF-8 byte-order mark
(`ef bb bf`) in front; before `Main` learned to drop it, the command above failed with
`Adding the user failed: A secret is 8 to 64 printable ASCII characters without spaces` (**[Verified — observed]**).
Running `--add-user` again for an existing participant replaces its secret.

### 4.2 Start the server

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal exchange.journal --users exchange.users
```

**[Verified — observed]** output on a fresh journal (two lines on stdout, then nothing until shutdown):

```
ExchangeServer2 listening on 127.0.0.1:9137, journal C:\...\exchange.journal
Recovered through sequence 0: replayed 0 commands, cut 0 torn bytes
```

The second line is the recovery summary (Spec D77): the last sequence number, the snapshot it started from if any,
how many commands were replayed, how many torn bytes were cut, plus the quarantined commands and a rules adoption
when there are any (**[Verified]** `src/main/java/exchangeserver/Main.java:152-157`). The gateway starts accepting
before replay; a client that connects during a long replay gets `ERROR exchange is starting; retry shortly`
(**[Verified]** `src/main/java/exchangeserver/Main.java:139`,
`src/main/java/exchangeserver/gateway/ClientSession.java:230`).

With TLS (**[Verified]** from code; the TLS path is exercised by `TcpGatewayTest.Security.tlsRoundTrip`, not by a
manual run):

```
set ES2_TLS_PASSWORD=...
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --bind 0.0.0.0 --tls-keystore server.p12 --users exchange.users
```

### 4.3 Files next to the journal

| File | Created by | What it is |
|---|---|---|
| `exchange.journal` | the first start | The write-ahead log (format v3, a CRC per record) |
| `exchange.journal.lock` | the first start | Empty. While the exchange runs it holds an exclusive OS lock on it, so a second exchange, `--quarantine` or `--compact` on the same journal is refused with "already open" (D65). Left on disk on purpose (deleting it could race with the next opener); a leftover from a killed process does not block the next start, because the OS releases the lock when the process dies |
| `exchange.journal.snapshot-N` | `--compact` | The state after sequence N (D71) |
| `exchange.journal.archive-A-B` | `--compact` | The journal records A..B that the snapshot replaced |
| `exchange.journal.tmp`, `*.snapshot-N.tmp` | write-then-rename | Only during an atomic write; normally gone |

**[Verified]** `src/main/java/exchangeserver/journal/JournalLock.java:37-54`,
`src/main/java/exchangeserver/core/Exchange.java:174-194`, `src/main/java/exchangeserver/journal/Durable.java:25-36`.
**[Verified — observed]** on Windows a hard-killed server left the lock file behind and restarted normally.

### 4.4 Connect a client

```
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain host 9000 --tls --truststore ca.p12
```

It prints `Connected to localhost:9000. Try: LOGIN alice <secret> / NEW c1 BUY LIMIT 10 100 / SUBSCRIBE / QUIT`
(plus ` (TLS)` with `--tls`), then echoes each server line prefixed with `< ` (**[Verified]**
`src/main/java/exchangeserver/client/ClientMain.java:65`). Without `--truststore`, `--tls` trusts the JDK's default
certificates. Any raw TCP tool also works for plain TCP (telnet, `nc`, PowerShell's `System.Net.Sockets.TcpClient`):
the protocol is ASCII lines ending in `\n` (`\r\n` accepted).

The protocol in one screen (**[Verified]** `src/main/java/exchangeserver/gateway/Protocol.java:20-44`):

```
Client -> server
    LOGIN <participant> <secret> [COD]          COD: cancel my resting orders when my last session closes (D72)
    NEW <clOrdId> <BUY|SELL> <LIMIT|IOC|FOK|POST_ONLY> <quantity> <price>
    NEW <clOrdId> <BUY|SELL> MARKET <quantity>
    CANCEL <orderId>
    RESEND <oseq>                               private events from oseq on, as far as still retained (D73)
    SUBSCRIBE | UNSUBSCRIBE | PING | QUIT
Server -> client
  Private stream, numbered per participant (oseq), sent to every session of that participant (D73):
    FILL oseq= orderId= clOrdId= side= price= qty= leaves= liquidity=TAKER|MAKER   (one per trade, both sides)
    ORDER oseq= orderId= clOrdId= status= filled= remaining= [reason=]           (the outcome of a NEW)
    CANCELLED oseq= orderId= qty= [reason=CANCEL_ALL]   CANCEL_REJECT oseq= orderId= reason=UNKNOWN_ORDER
    (a line repeated by RESEND ends with resend=1)
  Session:
    WELCOME participant=<p> oseq=<latest>       RESENT last=<latest>      ERROR RESEND_GAP first=<oldest retained>
    BOOK seq= last= bids=<price>x<qty>,... asks=...
    TRADE seq= price= qty= aggressor=          LEVEL seq= side= price= qty= orders=   (qty=0: level gone)
    SUBSCRIBED  UNSUBSCRIBED  PONG  BYE
    ERROR <text>
QUIT: every request sent before it is answered first, then BYE, then the server closes the connection.
```

### 4.5 A real session

**[Verified — observed]** 2026-10-27 against a fresh journal, driven by a script with several TCP connections
(`>` is what a client sent, `<` what it received; `mallory` is a connection trying Alice's name):

```
mallory > LOGIN alice guessed-secret
mallory < ERROR login failed
mallory < (closed)
alice > LOGIN alice alice-secret COD
alice < WELCOME participant=alice oseq=0
bob > LOGIN bob bob-secret!
bob < WELCOME participant=bob oseq=0
alice > NEW a1 SELL LIMIT 10 100
alice < ORDER oseq=1 orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10
alice > NEW a2 BUY LIMIT 5 100
alice < ORDER oseq=2 orderId=2 clOrdId=a2 status=CANCELLED filled=0 remaining=5 reason=SELF_TRADE_PREVENTED
bob > NEW b1 BUY LIMIT 4 100
bob < FILL oseq=1 orderId=3 clOrdId=b1 side=BUY price=100 qty=4 leaves=0 liquidity=TAKER
bob < ORDER oseq=1 orderId=3 clOrdId=b1 status=FILLED filled=4 remaining=0
alice < FILL oseq=3 orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER
alice > QUIT
alice < BYE
alice < (closed)
bob > SUBSCRIBE
bob < SUBSCRIBED
bob < BOOK seq=4 last=100 bids=- asks=-
alice2 > LOGIN alice alice-secret
alice2 < WELCOME participant=alice oseq=4
alice2 > RESEND 1
alice2 < ORDER oseq=1 orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10 resend=1
alice2 < ORDER oseq=2 orderId=2 clOrdId=a2 status=CANCELLED filled=0 remaining=5 reason=SELF_TRADE_PREVENTED resend=1
alice2 < FILL oseq=3 orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER resend=1
alice2 < CANCELLED oseq=4 orderId=1 qty=6 reason=CANCEL_ALL resend=1
alice2 < RESENT last=4
alice2 > QUIT
alice2 < BYE
alice2 < (closed)
bob > QUIT
bob < BYE
bob < (closed)
```

Things worth noticing (each is explained in guides 05 and 07):

- A wrong secret gets the same `ERROR login failed` as an unknown name, and the connection closes: one attempt per
  connection (Spec D74).
- Each participant has its own `oseq` sequence: Bob's first message is `oseq=1` although Alice is already at 2.
- `a2` would have traded with Alice's own resting sell: cancelled with `SELF_TRADE_PREVENTED`, nothing traded (D67).
  It still used up order id 2, which is why `b1` gets 3.
- Alice logged in with `COD`; when her last session quit, the gateway submitted a journalled `CANCEL_ALL alice`, so
  Bob's snapshot shows an empty book (D72).
- `RESEND 1` returned every private event, including the cancel she missed while away (D73).

The journal after that session (**[Verified — observed]**, plain text, each record ending with its CRC-32C):

```
HEADER,3,2,0,5d7826e0
NEW,1,2026-10-27T14:20:40.373493200Z,1,alice,a1,SELL,LIMIT,10,100,379beca7
NEW,2,2026-10-27T14:20:40.802670Z,2,alice,a2,BUY,LIMIT,5,100,f33577be
NEW,3,2026-10-27T14:20:41.217763100Z,3,bob,b1,BUY,LIMIT,4,100,bed0c339
CANCEL_ALL,4,2026-10-27T14:20:42.045241200Z,alice,2b6bb4a5
```

The header says format 3, rules version 2, base sequence 0 (no snapshot). The self-trade-prevented `a2` is
journalled because its outcome depended on the book; the failed login, `SUBSCRIBE`, `RESEND` and `QUIT` are not.

### 4.6 Compaction (snapshot + fresh journal)

**[Verified — observed]**, same journal as above. While the server was running:

```
> java -jar ... --journal exchange.journal --compact
Compaction failed: Journal C:\...\exchange.journal is already open (a running exchange, or a --quarantine or --compact run)
(exit code 1)
```

After stopping it:

```
> java -jar ... --journal exchange.journal --compact
Compacted through sequence 4: 0 resting orders in C:\...\exchange.journal.snapshot-4; old journal archived as C:\...\exchange.journal.archive-1-4
```

Files afterwards: `exchange.journal` (22 bytes: just the header, base 4), `exchange.journal.archive-1-4` (297
bytes), `exchange.journal.lock` (0), `exchange.journal.snapshot-4` (135), `exchange.users` (192). The snapshot:

```
SNAPSHOT,1,4,3,2026-10-27T14:20:42.045241200Z,100,88a18b6a6365ebe4,d7e6a970
OSEQ,alice,4,d1453b0b
OSEQ,bob,1,3d136969
END,0,2,6a6d3c70
```

(last sequence 4, last order id 3, last timestamp, last trade price 100, output digest; no resting orders; the last
`oseq` of each participant; an END record with the counts.) The restart then printed:

```
Recovered through sequence 4 from snapshot 4: replayed 0 commands, cut 0 torn bytes
```

When to compact (**[Verified]** README, Spec D71): at the end of a trading day (it bounds recovery time and ends
the scope of client order ids: after it, only resting orders keep theirs), and before moving to a build with a new
`Rules.VERSION` (compact with the **old** build, then start the new one, which adopts the empty journal). Compaction
itself replays the journal, so it must run with the build that wrote it.

### 4.7 Restart and crash recovery

**[Verified — observed]** earlier the same day (before the D66–D77 changes, so the lines had no `oseq` and the
journal no CRCs): a server was hard-killed, half a record was appended to simulate a crash mid-write, and the
restart cut the torn record, rebuilt the book and the last trade price, and continued the counters (next sequence 7,
next order id 5). With the final code the cut is reported in the recovery line (`cut N torn bytes`).

### 4.8 Startup failures (exit code 1)

`Main` prints `Startup failed: <message>` and then a hint that fits the failure (Spec D77; **[Verified]**
`src/main/java/exchangeserver/Main.java:143-149`, `186-198`):

| Failure | Message (from code) | Hint |
|---|---|---|
| No users file | `No users file C:\...\exchange.users: create it with --add-user <participant> --users ...` | (none: fails before the gateway is created) |
| Journal written under other rules, with records | `Journal ... was written under rules version 1; this build uses 2. Compact it with the build that wrote it (--compact); this build can then start from the compacted journal and its snapshot.` | `The journal was not changed.` |
| Not a journal | `Bad journal header "keep me": ...` or `Journal ... does not start with a journal header: "..."` | `The journal was not changed.` |
| CRLF-converted journal | `Bad journal header "...": record ends with '\r': the file was converted to CRLF line endings; convert it back to LF (e.g. dos2unix) and restart` | `The journal was not changed.` |
| Changed record (checksum) | `Corrupt journal record N: "..."` | `The journal was not changed.` |
| Engine throws on a replayed command | `Replay failed on command N (...)` | `Replay failed on command N (a poison pill?). Inspect it, then run --quarantine N and start again.` |
| A CHECK digest differs | `Replay diverged before CHECK N: ...` | `This build replays the journal differently from the build that wrote it. Start the old build, run --compact, then start this build.` |
| Journal open elsewhere | `Journal ... is already open (a running exchange, or a --quarantine or --compact run)` | `The journal was not changed.` |

**[Verified]** messages at `src/main/java/exchangeserver/gateway/Credentials.java:58`,
`src/main/java/exchangeserver/journal/CommandJournal.java:248`, `255-257`,
`src/main/java/exchangeserver/journal/JournalCodec.java:83`, `174-175`,
`src/main/java/exchangeserver/core/Replay.java:82`, `93`. Before the D69 change, a CRLF journal and a non-journal
file were observed failing with older wording; the hints are covered by `MainTest`.

### 4.9 Quarantining a poison pill

If startup fails while replaying one command, the hint names it. After inspecting the record:

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal exchange.journal --quarantine 7
```

Output on success: `Quarantined command 7 in C:\...\exchange.journal`. The journal gains `SKIP,<next seq>,<now>,7`
with its CRC; the next startup skips record 7. A second `--quarantine 7` is refused
(`Command 7 is already quarantined`). While a server has the journal open the tool is refused (D65);
**[Verified — observed]** on Linux in Docker:

```
Quarantine failed: Journal /tmp/j.journal is already open (a running exchange, or a --quarantine or --compact run)
exit: 1
```

### 4.10 Stopping, and exit codes

| How it ends | Exit code | What happens |
|---|---|---|
| Ctrl+C / SIGINT / SIGTERM | **0** | Shutdown hook: close the gateway (drop connections; COD participants' last sessions close, so their `CANCEL_ALL` is accepted before the stop), `stop()` the service (refuse new requests, process everything already accepted, close the journal), wait up to 10 s, then `Runtime.halt(0)` (**[Verified]** `src/main/java/exchangeserver/Main.java:159-177`). **[Verified — observed]** on Linux in Docker: exit status 0 after SIGINT and after SIGTERM. On Windows Ctrl+C was not exercised. |
| Writer not stopped within 10 s | 1 | `Writer thread did not stop within 10 s` |
| Halt (journal I/O failure, `Error` in the engine, writer interrupted) | 2 | `Exchange halted: <cause>` on stderr, `System.exit(2)`; the hook then halts with 2 (**[Verified]** `src/main/java/exchangeserver/Main.java:179-183`) |
| Bad arguments, startup failure, failed tool | 1 | One-line message on stderr |
| Hard kill (`kill -9`, Task Manager) | n/a | Nothing runs; the journal is the truth; restart replays it |

A practical trap, **[Verified — observed]** in Docker: a server started in the background (`java ... &`) from a
non-interactive bash script **ignores SIGINT** (POSIX: background jobs without job control start with SIGINT
ignored; the process's `SigIgn` mask showed signal 2), so `kill -INT` does nothing there. Use `set -m` in the
script, or SIGTERM.

## 5. Tests

```
mvnw.cmd test                                           all tests
mvnw.cmd test -Dtest=MatchingEngineTest                 one class, including its @Nested groups
mvnw.cmd test -Dtest=RulesTest#oppositeSide             one test method
mvnw.cmd test "-Dtest=ExchangeServiceTest,TcpGatewayTest"   several classes
```

**[Verified]** commands from `CLAUDE.md`; the multi-class form was used during the work. In PowerShell, quote `-D`
arguments containing commas, `#` or `$`. **[Inferred]** a single nested group can be selected as
`"-Dtest=ExchangeServiceTest`$Batching"` (Surefire treats nested classes as `Outer$Inner`); not tried.

Expected end of output (**[Verified — observed]** on Windows; the Linux run, before the last 7 tests were added, showed 446):

```
[INFO] Tests run: 453, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

In the Linux container the test phase took about 55 s (**[Verified — observed]** `Total time: 54.691 s`).

**Expected noise** (not failures, **[Verified]** `CLAUDE.md` and observed): tests that inject poison pills or fatal
errors log stack traces, e.g. `SEVERE: Exchange halted` with `java.lang.OutOfMemoryError: simulated`, and
`simulated engine bug`; the callback test logs `Request callback failed`. Under PowerShell, anything a native program
writes to stderr is shown as `NativeCommandError`; that is PowerShell's wrapping, not a failure. Some tests start a
child JVM or run `keytool`, so a JDK (not just a JRE) must be the `java` on the path.

**Reports:** `target/surefire-reports/` (one `.txt` and `.xml` per test class). Only the last run's reports are
kept, so to study an intermittent failure save each run's console output.

### 5.1 Running the suite on Linux (Docker)

**[Verified — observed]** on 2026-10-27 with Docker Desktop (WSL2 backend): a script mounted the repo read-only at
`/src`, copied it without `target/` and `.git/` into `/w`, converted `mvnw` to LF, and ran `./mvnw -B test`; 446 tests
passed (Spec §11). An equivalent one-liner (**[Inferred]** equivalent to that script; replace the path):

```
docker run --rm -v "S:\path\to\ExchangeServer2:/src:ro" eclipse-temurin:17 bash -c "mkdir -p /w && (cd /src && tar --exclude=./target --exclude=./.git -cf - .) | tar -xf - -C /w && cd /w && sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw -B test"
```

Why copy: the container must not write the Windows `target/` while another build might use it, and `mvnw` checked
out with CRLF would not run under `sh` (`.gitattributes` now forces LF for `mvnw` on new checkouts).

## 6. Mutation testing (PIT)

```
mvnw.cmd -Pmutation test
```

Runs the normal tests, then PIT over `domain`, `book`, `engine`, `command`, `journal`, `core` with 4 threads; takes
several minutes. Report: `target/pit-reports/index.html` (**[Verified]** `pom.xml:127-169`, README). The project
rule: after a behavioural change, run it and explain survivors in Spec §7; fsync (`force`), directory fsync and log
calls are expected survivors because no unit test can observe them, and the branch of `JournalLock.acquire` where
another process holds the lock runs only in a child JVM that PIT does not instrument (**[Verified]** `CLAUDE.md`,
Spec §7).

## 7. Benchmarks (JMH)

```
mvnw.cmd -Pbench verify -Djmh.args="MatchingBenchmark -f 1 -wi 3 -w 1s -i 5 -r 1s"
mvnw.cmd -Pbench verify -Djmh.args="BatchingBenchmark -f 1 -wi 2 -w 2s -i 3 -r 3s -t 16"
```

`-f` forks, `-wi`/`-w` warm-up iterations/time, `-i`/`-r` measured iterations/time, `-t` threads (= simulated
clients for `BatchingBenchmark`). The `bench` profile skips tests. `BatchingBenchmark` writes real journals under
`target/bench/` because the cost of fsync is the point (**[Verified]**
`src/test/java/exchangeserver/bench/BatchingBenchmark.java:25-39`). **Not run** on 2026-10-27; recorded results are
in Spec §5 and predate D66–D77 (e.g. a command costs ~0.65–1.1 µs; smart batching reaches 43 orders/ms with 64
clients).

## 8. Debugging

### 8.1 Threads you will see (names are set in code)

| Thread | Daemon? | Role | Where |
|---|---|---|---|
| `main` | no | Runs `Main`, then waits in `awaitTermination()` | `src/main/java/exchangeserver/Main.java:179` |
| `exchange-writer` | **no** | The single writer: batches, journal, ledger, listener, callbacks | `src/main/java/exchangeserver/core/ExchangeService.java:119-121` |
| `gateway-acceptor` | yes | `accept()` loop | `src/main/java/exchangeserver/gateway/TcpGateway.java:144-145` |
| `session-N-reader` | yes | Login, parses lines from client N, limits, hands in requests | `src/main/java/exchangeserver/gateway/ClientSession.java:91`, `93` |
| `session-N-sender` | yes | Writes client N's outbound queue to its socket | `src/main/java/exchangeserver/gateway/ClientSession.java:92`, `94` |
| `ForkJoinPool.commonPool-worker-*` | yes | Completes futures handed to API callers (D61) | `src/main/java/exchangeserver/core/ExchangeService.java:170-180` |
| `shutdown` | no | The shutdown hook | `src/main/java/exchangeserver/Main.java:159-177` |

A thread dump (`jstack <pid>` or `jcmd <pid> Thread.print`) shows instantly where each is blocked. An idle exchange
has `exchange-writer` waiting in `LinkedBlockingQueue.take`, readers in a socket read (with a timeout), senders in
`ArrayBlockingQueue.take`. **[Inferred]** from the code paths; not captured.

### 8.2 Good breakpoints

| To see… | Break at |
|---|---|
| A batch being stamped and journalled | `src/main/java/exchangeserver/core/Exchange.java:198` (`process`) and `:223` (`journal.appendAll`) |
| One order being matched | `src/main/java/exchangeserver/engine/MatchingEngine.java:178` (`process`), the loop at `:219`, self-trade stop at `:225` |
| What a client line becomes | `src/main/java/exchangeserver/gateway/ClientSession.java:211` (`handle`) |
| A login | `src/main/java/exchangeserver/gateway/ClientSession.java:293` (`login`) |
| Lines leaving for a client | `ClientSession.send` at `src/main/java/exchangeserver/gateway/ClientSession.java:116` |
| Replay at startup | `src/main/java/exchangeserver/core/Replay.java:39` (`run`) |
| A poison pill | `src/main/java/exchangeserver/core/Exchange.java:278` (`quarantine`) |
| Compaction | `src/main/java/exchangeserver/core/Exchange.java:174` (`compact`) |

Caution: **a breakpoint on the writer thread freezes the whole exchange** (by design nothing else touches the
book), and if it sits long enough every client's requests pile up until `SESSION_BUSY` or BUSY, and silent sessions
hit their idle timeout. Prefer "suspend thread" over "suspend all", and prefer tests with the `CommandInterceptor`
seam for reproducible pauses (guide 10).

To debug a running jar from an IDE, start it with the standard JDWP agent (**[General knowledge]**):

```
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005 -jar target/exchange-server2-2.0.0-SNAPSHOT.jar
```

### 8.3 Logs

Only three classes log, through `System.Logger`, which (with no logging library) goes to `java.util.logging` and
prints to **stderr** (**[Verified]** guide 02 §6). `Main` prints its startup lines and failures directly.

| Message | Level | Meaning | Source |
|---|---|---|---|
| `Engine failed on command N; quarantining it` + stack trace | ERROR (shown as `SEVERE`) | A poison pill was quarantined live | `src/main/java/exchangeserver/core/Exchange.java:279` |
| `Output listener failed on command N` | ERROR | The gateway's listener threw; ignored | `src/main/java/exchangeserver/core/Exchange.java:249` |
| `Request callback failed` | ERROR | A request callback threw; logged instead of vanishing (D77) | `src/main/java/exchangeserver/core/ExchangeService.java:218` |
| `Exchange halted` + cause | ERROR | Writer halted; process will exit 2 | `src/main/java/exchangeserver/core/ExchangeService.java:343` |
| `Closing the journal failed` | WARNING | | `src/main/java/exchangeserver/core/ExchangeService.java:315` |
| `accept failed N time(s) in a row; retrying` | WARNING, ERROR from the 10th | Accept error (e.g. too many open files); back-off 100 ms doubling to 5 s | `src/main/java/exchangeserver/gateway/TcpGateway.java:178-181` |
| `cancel-on-disconnect for P failed: ...` | WARNING | The CANCEL_ALL was refused (e.g. the exchange is stopping) | `src/main/java/exchangeserver/gateway/TcpGateway.java:259` |
| `session N is too slow; disconnecting` | INFO | A client's outbound queue filled | `src/main/java/exchangeserver/gateway/ClientSession.java:121` |

There is no request log and no metrics. Secrets are never logged (`ClientMessage.Login.toString` omits them). To
change log levels, pass a `java.util.logging` config file with `-Djava.util.logging.config.file=...`
(**[General knowledge]**).

### 8.4 Inspecting state

- **The journal is the best debugging tool**: plain text, one command per line, in processing order, each with a
  CRC. Replaying it in a test (`Exchange.open(path, clock)` then `snapshot(depth)`) rebuilds the exact book. After
  a compaction, look at the snapshot file and the archive too.
- **The live book**: `SUBSCRIBE` from any client gives a `BOOK` snapshot (best 100 levels per side).
- **A participant's history**: log in as it and `RESEND 1` (up to the retained 1,000 events since the last
  compaction).
- From code (tests): go through the queue (`service.snapshot(depth)`, `service.resend(p, n)`), never read the
  `OrderBook` from another thread (**[Verified]** `CLAUDE.md`, "Single writer").

## 9. Windows-specific notes

**[Verified]** from `CLAUDE.md`, Spec §1.5 and observations unless marked:

- The default charset is windows-1252; the code always passes a charset, and the build sets UTF-8. Do the same in
  any code you add.
- **Never write Java sources with Windows PowerShell's `Set-Content -Encoding utf8`**: it adds a byte-order mark,
  which `javac` rejects. Use an editor, or `[IO.File]::WriteAllText(path, text, New-Object Text.UTF8Encoding($false))`.
- PowerShell pipes text to programs with a byte-order mark (`--add-user` now strips it), and `Set-Content`/`Out-File`
  write CRLF line endings: fatal for a journal, which now says so explicitly (§4.8). `.gitattributes` keeps git from
  converting journals.
- Java opens files with delete sharing on Windows, so deleting a file does not prove it was closed; tests use
  `CommandJournal.openJournals()` instead.
- File locks differ by OS: on Windows they are mandatory and belong to a handle; on Linux/macOS they are advisory,
  belong to the process, and are dropped when *any* handle on the file closes. That is why the exchange locks a
  separate `<journal>.lock` file that nothing else opens (D65; guide 02 §3.3). The cross-process test has run on both.
- Windows cannot open a directory as a file, so the directory fsync after creating or renaming a file is skipped
  there (NTFS journals its metadata itself) (**[Verified]** `src/main/java/exchangeserver/journal/Durable.java:46-63`).
- One fsync costs ~1.5 ms on the dev disk; timed waits round up to the ~15.6 ms Windows timer (why the linger
  batching mode measured badly, Spec §5).
- The stale-build trap (§3).

## 10. Deployment

There is no container image, service definition or CI configuration (**[Verified]** no such files in the repo); the
Docker run in §5.1 is a manual check. The deliverable is the jar, a users file and a journal. **[Inferred]** a real
deployment would need at least: a process supervisor that restarts on exit code 2 (but not blindly on exit 1, which
needs an operator); the journal, snapshots and archives on a durable local disk with a backup policy; a scheduled
`--compact` while the exchange is stopped (e.g. end of day); TLS with a proper certificate if `--bind` exposes the
port; protection of the users file and of the TLS password in the environment.

## Key takeaways

- Only JDK 17 is needed; the wrapper fetches Maven.
- Before the first start, create the users file with `--add-user`; the server binds 127.0.0.1 unless `--bind` says
  otherwise; TLS with `--tls-keystore` and `ES2_TLS_PASSWORD`.
- Flags cover port, bind, journal, users, TLS, timeouts, per-session limits, CHECK interval and RESEND window; rule
  values are code constants certified by `Rules.VERSION` (2).
- Startup prints `Recovered through sequence N [from snapshot B]: replayed R commands, cut T torn bytes`.
- Operator tools: `--quarantine <seq>` and `--compact`, both refused while the exchange runs; compaction leaves a
  snapshot and an archive next to the journal.
- Exit codes: 0 clean stop (observed after SIGINT and SIGTERM on Linux), 1 startup or tool failure, 2 halted.
- Tests: `mvnw.cmd test` → 453 green on Windows (446 in the Linux run); stack traces from simulated failures are expected noise.
- Debug by thread name; a breakpoint on `exchange-writer` freezes the exchange; the journal (plus snapshot) is the
  best state inspector.

## Common confusions

- **"The server starts without users."** No: `Credentials.load` fails if the users file is missing, and `Main` exits
  1 with a hint to run `--add-user`.
- **"`--bind 0.0.0.0` is enough to go on the network."** It works, but without `--tls-keystore` secrets and fills
  travel in clear text; `Main` warns about it.
- **"Exit code 1 means the exchange crashed."** No: 1 means it never started (or a tool refused). A running exchange
  that fails halts with exit code 2.
- **"The tool printed `Quarantined command 7`, so the poison is gone."** The poison record stays in the journal
  (history is annotated, never edited); a `SKIP` record after it tells replay to pass over it. Compaction later moves
  it into the archive.
- **"A leftover `exchange.journal.lock` means the exchange is still running."** No: the file always stays. Only a
  live OS lock on it means "in use".
- **"After `--compact` the history is gone."** It is in `exchange.journal.archive-A-B`; the snapshot plus the new
  journal replace it for recovery, but nothing is deleted.
- **"`kill -INT` from my script did nothing, so the shutdown hook is broken."** Background jobs of a
  non-interactive shell ignore SIGINT; use SIGTERM or `set -m`.
- **"`-DskipTests` skips compiling tests."** It skips running them; test sources (and the JMH processor) still
  compile. **[General knowledge]** (`-Dmaven.test.skip=true` skips both.)

## Questions to test yourself

1. You start the jar twice on the same journal with different ports. What happens to the second?
   <details><summary>Answer</summary>It binds its port and starts accepting, then `CommandJournal.open` fails to
   take the lock on `exchange.journal.lock` and throws `IllegalStateException("... is already open ...")`; `Main`
   closes the gateway, prints `Startup failed: ...` and `The journal was not changed.`, and exits 1 (D65).</details>
2. In the session above, why does `b1` get order id 3 although only one order rests before it?
   <details><summary>Answer</summary>Order ids are issued by the sequencer to every journalled new order, including
   Alice's self-trade-prevented `a2` (id 2). State-dependent outcomes are journalled and use up an id.</details>
3. Which lines of the session produced no journal record, and why?
   <details><summary>Answer</summary>The failed `LOGIN`, the `LOGIN`s, `SUBSCRIBE`, `RESEND`, `QUIT`: session-level
   messages and reads never reach the journal. The `CANCEL_ALL` that `COD` triggered is journalled, although no
   client typed it.</details>
4. A colleague runs `--compact` with the new build (rules version 3) on a journal written under rules version 2.
   What happens, and what should they do?
   <details><summary>Answer</summary>`CommandJournal.open` refuses: the journal has records written under another
   rules version, and compaction replays them. They must compact with the build that wrote the journal (rules 2);
   the new build then adopts the empty compacted journal and starts from the snapshot.</details>
5. Alice's client reconnects after a network cut and sees `WELCOME participant=alice oseq=12`. Its last line was
   `oseq=9`. What should it send, and what can it receive?
   <details><summary>Answer</summary>`RESEND 10`. It gets the events 10–12 marked `resend=1` and `RESENT last=12`;
   if some were older than the retained window (or than the last compaction), first `ERROR RESEND_GAP first=N`.</details>
6. How do you make `MatchingEngineTest` stop at the start of every order match in your IDE without freezing any
   real clients?
   <details><summary>Answer</summary>Run the test in debug mode with a breakpoint at `MatchingEngine.process`
   (`src/main/java/exchangeserver/engine/MatchingEngine.java:178`); the engine tests call the engine directly on the
   test thread, with no writer thread or sockets involved.</details>
