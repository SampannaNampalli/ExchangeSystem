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

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal exchange.journal
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000
```

A session:

```
> LOGIN alice
< WELCOME participant=alice
> NEW a1 SELL LIMIT 10 100
< ORDER orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10
> SUBSCRIBE
< SUBSCRIBED
< BOOK seq=1 last=0 bids=- asks=100x10
        (bob, elsewhere: NEW b1 BUY LIMIT 4 101)
< FILL orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER
< TRADE seq=2 price=100 qty=4 aggressor=BUY
< LEVEL seq=2 side=SELL price=100 qty=6 orders=1
> CANCEL 1
< CANCELLED orderId=1 qty=6
< LEVEL seq=3 side=SELL price=100 qty=0 orders=0
```

The full protocol is documented at the top of `src/main/java/exchangeserver/gateway/Protocol.java`.

Stop with Ctrl+C: the gateway closes, every accepted request is processed, the journal is closed. Restarting on
the same journal rebuilds the identical book. Exit code 2 means the exchange halted (for example a journal I/O
error); restart it. If startup fails on one command (a poison pill), inspect it, then:

```
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal exchange.journal --quarantine <seq>
```

## Layout

| Package | Contents |
|---|---|
| `domain` | `Side`, `OrderType`, `Rules` (every limit replay depends on), `OrderRequest` (stateless validation), `Order` |
| `book` | `OrderBook`, `PriceLevel` (read-only to everyone else), `OrderNode`, `Fill`, `LevelUpdate`, `CancelResult` |
| `engine` | `MatchingEngine` (matching, risk checks, outputs), `Response`, `Trade`, `Execution`, `BookSnapshot` |
| `command` | `Command` (sealed: new order, cancel, skip), `Sequencer` (the only clock reader / counter) |
| `journal` | `CommandJournal` (write-ahead log, torn-tail recovery, fail-stop), `JournalCodec` (the text format) |
| `core` | `Exchange` (stamp, journal, process; poison-pill policy), `Replay`, `ExchangeService` (the writer thread) |
| `gateway` | `TcpGateway`, `ClientSession`, `Protocol` |
| `client` | `ExchangeClient`, `ClientMain` |

Tests mirror the packages: one test class per production class, `@Nested` groups per behaviour.
