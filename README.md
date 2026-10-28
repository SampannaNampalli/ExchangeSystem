# ExchangeServer2

A crash-safe, deterministic exchange matching engine in Java 17: a single-instrument limit order book with
price-time-priority matching, a checksummed write-ahead journal with replay and snapshots, a single-writer thread
model, and an authenticated TCP/TLS gateway. It uses only the JDK at runtime (JUnit and JMH are test-only).

```
client --TCP/TLS--> gateway (auth, stateless checks, rate limits) --queue--> writer thread:
        stamp (sequencer) -> journal (one write + one fsync per batch) -> matching engine -> order book
                                  |                                              |
                    replay + snapshot on restart                 replies, fills, market data
```

## At a glance

| | |
|---|---|
| **Engine latency** | ~0.65–1.1 µs per command, flat from 10 to 100,000 price levels per side (JMH) |
| **Durable throughput** | 43,300 fsynced orders/s with 64 clients, **53x** fsync-per-command, p50 1.4 ms / p99 2.4 ms |
| **Tests** | 453 JUnit 5 tests, all green on Windows and on Linux (Docker) |
| **Mutation testing** | PIT: 702 mutants, 92% killed, 94% test strength, 94% line coverage; every survivor explained |
| **Randomised checking** | book invariants re-derived from scratch after each of 30,000 random engine commands |
| **Design record** | 77 numbered design decisions, each with its reason ([Spec §3](docs/ExchangeServer2_Spec.md)) |
| **Code size** | ~5,700 lines of production Java (62 files), ~5,700 lines of tests (26 files); warning-free under `-Xlint:all` |
| **Runtime dependencies** | none beyond the JDK |

Benchmarks ran on a laptop (Windows 11, JDK 17, JMH 1.37; one fsync costs ~1.5 ms on that disk), so treat them as
indicative. Full tables: [Spec §5](docs/ExchangeServer2_Spec.md).

## Highlights

**Matching engine**
- Price-time priority with five order types: LIMIT, IOC, FOK, MARKET and POST_ONLY.
- O(1) cancel: each price level is a doubly linked list with sentinel nodes, plus an `orderId -> node` map.
  Best-price lookup is O(log L) on a `TreeMap`, measured at 8–18 ns, under 2% of a command.
- Pre-trade risk checks: ±10% price band and market-order collar, duplicate client-order-id detection, per-participant
  and global resting-order caps, and self-trade prevention (cancel the incoming remainder).
- The compiler enforces ownership: every `PriceLevel` mutator is package-private, so the engine can read the book but
  can only change it through methods that keep its invariants.

**Durability and recovery**
- Write-ahead journal: no reply, fill or market-data message exists for a command that is not fsynced.
- **Smart batching** (take whatever is waiting, one fsync, then process): the same latency as fsync-per-command at low
  load, and throughput that grows with load. A timer-based "linger" batch measured worse, because Windows rounds
  timed waits up to ~15.6 ms.
- Deterministic replay: the engine reads no clock and has no counters, randomness or hash-iteration order, so a
  restart rebuilds the identical book. A periodic **output digest** (FNV-1a over every engine output) is journalled
  and compared during replay, so a code change that alters behaviour is detected, not replayed silently.
- CRC-32C on every journal and snapshot record, directory fsync, torn-tail repair, atomic file replacement and
  crash-safe compaction into snapshots, which bound recovery time.
- Fail-stop with a poison-pill policy: a command that crashes the engine is quarantined (`SKIP`) and the engine is
  rebuilt by replay; an I/O failure halts the process (exit code 2) rather than continuing in an unknown state.
- One process per journal, enforced by a lock file that works on both Windows (mandatory locks) and POSIX (where
  closing any handle drops a process's locks).

**Concurrency**
- One writer thread owns the sequencer, journal, engine and book: no locks in the hot path and no data races by
  construction. Other threads, reads included, reach the state only through a queue, and each caller gets its own
  `CompletableFuture`.
- Shutdown never loses an accepted request (tested with 5 × 1,000 requests racing `stop()`).
- Backpressure: a full queue is refused with `BUSY` and a slow consumer is disconnected, so neither can stall the
  writer.

**Gateway and security**
- Line-based TCP protocol, optional TLS 1.3/1.2; binds to loopback unless told otherwise.
- Salted PBKDF2-HMAC-SHA256 credentials with constant-time comparison.
- Per-session token-bucket rate limit, in-flight limit, login and idle timeouts, and line-length limits.
- A numbered private stream per participant with `RESEND` after reconnecting, and opt-in cancel-on-disconnect.

**Engineering process**
- Every behaviour change goes through mutation testing; surviving mutants are either killed with a new test or
  explained (fsync and log calls, equivalent boundaries) in [Spec §7](docs/ExchangeServer2_Spec.md).
- Optimisations are measured before they are kept: a cached best-level pointer was benchmarked and rejected; smart
  batching was benchmarked and kept.
- A full code review produced 10 findings (concurrency races, resource leaks, a lost-reply bug). Each fix that a test
  can observe has a regression test, and each of those tests was confirmed to fail against the old code.
- Cross-platform checks: running on Linux exposed a file-locking difference between Windows and POSIX, which a
  child-JVM test now covers on both.

## Build and test

JDK 17. Maven is not needed: the wrapper downloads it.

```
mvnw.cmd test                 (Windows)      ./mvnw test      (macOS/Linux)
mvnw.cmd package              builds target/exchange-server2-2.0.0-SNAPSHOT.jar
mvnw.cmd -Pmutation test      mutation testing (PIT), report in target/pit-reports/index.html
mvnw.cmd -Pbench verify -Djmh.args="MatchingBenchmark -f 1 -wi 3 -w 1s -i 5 -r 1s"
mvnw.cmd -Pbench verify -Djmh.args="BatchingBenchmark -f 1 -wi 2 -w 2s -i 3 -r 3s -t 16"
```

IntelliJ: File > Open > `pom.xml`.

## Run

Participants log in with a secret (Spec D74). Create the users file first (the secret is asked for, or read from
standard input), then start the server. It listens on 127.0.0.1 unless `--bind` says otherwise; with `--bind`, use
TLS (`--tls-keystore server.p12`, password in the `ES2_TLS_PASSWORD` environment variable). Every option is listed
at the top of `src/main/java/exchangeserver/Main.java`.

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --users exchange.users --add-user alice
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal exchange.journal --users exchange.users
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000 [--tls --truststore ca.p12]
```

A session (private lines carry `oseq`, alice's own numbered stream; `COD` = cancel her orders if she disconnects):

```
> LOGIN alice alice-secret COD
< WELCOME participant=alice oseq=0
> NEW a1 SELL LIMIT 10 100
< ORDER oseq=1 orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10
> SUBSCRIBE
< SUBSCRIBED
< BOOK seq=1 last=0 bids=- asks=100x10
        (bob, elsewhere: NEW b1 BUY LIMIT 4 101)
< FILL oseq=2 orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER
< TRADE seq=2 price=100 qty=4 aggressor=BUY
< LEVEL seq=2 side=SELL price=100 qty=6 orders=1
> NEW a2 BUY LIMIT 5 100
< ORDER oseq=3 orderId=3 clOrdId=a2 status=CANCELLED filled=0 remaining=5 reason=SELF_TRADE_PREVENTED
> CANCEL 1
< CANCELLED oseq=4 orderId=1 qty=6
< LEVEL seq=4 side=SELL price=100 qty=0 orders=0
        (alice reconnects later)
> RESEND 3
< ORDER oseq=3 ... resend=1
< CANCELLED oseq=4 orderId=1 qty=6 resend=1
< RESENT last=4
```

The full protocol is documented at the top of `src/main/java/exchangeserver/gateway/Protocol.java`.

Stop with Ctrl+C (exit code 0): the gateway closes, every accepted request is processed, the journal is closed.
Restarting on the same journal rebuilds the identical book. Startup prints what recovery did. Exit code 2 means
the exchange halted (for example a journal I/O error); restart it. Operator tools, run while the exchange is
stopped:

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal exchange.journal --compact          (snapshot + empty journal)
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal exchange.journal --quarantine <seq> (skip a poison pill)
```

Compact at the end of a trading day (it bounds recovery time and ends the scope of client order ids), and before
moving to a build with a new `Rules.VERSION`: compact with the old build, then start the new one.

## Layout

| Package | Contents |
|---|---|
| `domain` | `Side`, `OrderType`, `Rules` (every limit replay depends on), `OrderRequest` (stateless validation), `Order` |
| `book` | `OrderBook`, `PriceLevel` (read-only to everyone else), `OrderNode`, `Fill`, `LevelUpdate`, `CancelResult` |
| `engine` | `MatchingEngine` (matching, risk checks, self-trade prevention, outputs, state export/restore), `Response`, `Reason`, `Trade`, `Execution`, `BookSnapshot` |
| `command` | `Command` (sealed: new order, cancel, cancel-all, skip, check), `Sequencer` (the only clock reader / counter) |
| `journal` | `CommandJournal` (write-ahead log, checksums, torn-tail recovery, fail-stop), `JournalCodec` (the text format), `JournalLock`, `Durable` |
| `core` | `Exchange` (stamp, journal, process; poison-pill policy; compaction), `Replay`, `Ledger` (engine + output digest + private streams), `Snapshot`, `ExchangeService` (the writer thread) |
| `gateway` | `TcpGateway`, `ClientSession` (login, limits, timeouts), `Protocol`, `Credentials`, `Tls` |
| `client` | `ExchangeClient`, `ClientMain` |

Tests mirror the packages: one test class per production class, `@Nested` groups per behaviour.

## Documentation

- [docs/ExchangeServer2_Vision.md](docs/ExchangeServer2_Vision.md): purpose, engineering principles, non-goals and
  the roadmap.
- [docs/ExchangeServer2_Spec.md](docs/ExchangeServer2_Spec.md): the exact code state, the 77 design decisions with
  their reasons, the journal format and failure model, benchmark and mutation-testing results, and known
  limitations.
- [docs/understanding/](docs/understanding/00-START-HERE.md): study guides that walk through the code.

**Known limitations** (deliberate scope; see Spec §8): one instrument, one matching thread, offline compaction, two
platform threads per connection (256 sessions max), and no replication or failover.
