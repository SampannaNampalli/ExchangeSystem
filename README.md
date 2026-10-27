# ExchangeServer2

A single-instrument limit order book with a price-time-priority matching engine, a write-ahead journal with
replay, a single-writer thread model and a plain TCP gateway. A rewrite of `ExchangeServer` that follows
`docs/ExchangeServer2_Vision.md` (purpose, principles, roadmap) and `docs/ExchangeServer2_Spec.md` (code state,
decisions, open threads).

```
client --TCP--> gateway (stateless checks) --queue--> writer thread: stamp -> journal (fsync per batch) -> engine -> book
                                                                    |                                        |
                                                             replay on restart              replies, fills, market data
```

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
