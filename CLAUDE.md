# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

ExchangeServer2: a Java 17 single-instrument limit order book with a price-time matching engine, a write-ahead
journal with replay, a single-writer thread model and a plain TCP gateway. It is a learning project: depth of
understanding and justified design matter more than features. It is a rewrite of the user's earlier project at
`S:\Codes and Programs\Programs\Dev\Exchange Server` (default package, no build file), which is left untouched.

The two design documents are the source of truth and must be kept current when behaviour changes:
- `docs/ExchangeServer2_Vision.md`: purpose, working style, principles, and **the roadmap (it lives only there)**.
- `docs/ExchangeServer2_Spec.md`: exact code state, numbered decisions (D1–D77), journal format, benchmark and
  mutation results, open threads. New design decisions get the next D-number there, with their real reason.
  Comments that cite a "threads guide" or "Spec v8" point to files in the original project (Spec §1.6).

`docs/understanding/` holds study guides written for the user (start at `00-START-HERE.md`); they cite
`path:LINE`, so update them when behaviour or line numbers change, then regenerate `FULL-CONTEXT.md` with
`java docs/understanding/build-full-context.java`.

**Working style:** this rewrite was written by Claude at the user's explicit request. The original project's rule
was "the user writes production code; Claude reviews, quizzes, and writes the tests". For new work, ask which
mode applies instead of assuming (Vision §3).

## Commands

No local Maven: use the wrapper (`mvnw.cmd` on Windows, `./mvnw` elsewhere). JDK 17.

```
mvnw.cmd test                                   all tests (453)
mvnw.cmd test -Dtest=MatchingEngineTest         one test class, including its @Nested groups
mvnw.cmd test -Dtest=RulesTest#oppositeSide     one test method
mvnw.cmd package                                jar: target/exchange-server2-2.0.0-SNAPSHOT.jar
mvnw.cmd -Pmutation test                        PIT mutation testing -> target/pit-reports/index.html (several minutes)
mvnw.cmd -Pbench verify -Djmh.args="MatchingBenchmark -f 1 -wi 3 -w 1s -i 5 -r 1s"
mvnw.cmd -Pbench verify -Djmh.args="BatchingBenchmark -f 1 -wi 2 -w 2s -i 3 -r 3s -t 16"
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --users exchange.users --add-user alice    (secret on stdin)
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --port 9000 --journal exchange.journal --users exchange.users
java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --journal exchange.journal --compact         (exchange stopped)
java -cp target/exchange-server2-2.0.0-SNAPSHOT.jar exchangeserver.client.ClientMain localhost 9000
docker run --rm -v "<repo>:/src:ro" eclipse-temurin:17 ...      Linux run: copy /src, LF the mvnw, ./mvnw test (Spec §11)
```

The compiler runs with `-Xlint:all`; keep the build warning-free. Benchmarks live in
`src/test/java/exchangeserver/bench` and are excluded from surefire. Do not run two Maven builds at once (they
share `target/`). The wire protocol is documented at the top of `gateway/Protocol.java`.

## Architecture (the parts that span several files)

**The one live path.** `gateway.ClientSession` (reader thread) authenticates the session (`Credentials`), parses a
line and builds an `OrderRequest`, which is the *only* layer of field validation; invalid or over-limit requests
are answered with `ERROR` and never reach the queue. `core.ExchangeService` puts a `Request` plus its own
`CompletableFuture` on one queue. The single writer thread ("exchange-writer") takes a batch and calls
`core.Exchange.process`: stamp every request with `command.Sequencer` (the only clock reader and counter; a CHECK
first when due) -> `journal.CommandJournal.appendAll` (one write, one fsync per batch) -> feed each command to the
`core.Ledger` (engine + output digest + numbered private events) -> call the `OutputListener` (the participant's
private stream and market data; the gateway implements it) -> complete the future (D76: callbacks run after the
request's lines went out). Nothing is processed before its batch is fsynced.

**Determinism is load-bearing.** Replay feeds journalled commands through the same engine, so the engine must
depend only on its commands: no clock, no counters, no randomness, no hash-iteration order. Ids, sequence numbers
and timestamps come from the `Sequencer`; trade timestamps are the incoming command's timestamp.

**Ownership is enforced by packages.** Every `book.PriceLevel` mutator is package-private; the engine (another
package) can only read levels and must call `OrderBook.fillBest`/`add`/`cancel`, which keep the book invariants
(id map iff resting, no empty or crossed levels, per-participant open counts). Keep it that way; don't widen
visibility to make something convenient.

**Single writer.** Only the writer thread touches `Exchange`, `Sequencer`, `CommandJournal`, `MatchingEngine` and
`OrderBook`. Other threads reach state only through the queue (reads such as `snapshot` included). Tests that
inspect state must go through a future (a happens-before edge), never read the book directly from another thread.
`ExchangeService`'s state check, enqueue and `stop()` share one lock so no accepted request is ever lost; only
the writer closes the journal, as its last act. Callers get their own future (never the writer's), and the writer
decides what to process from its own state, never from a future's (D61).

**Recovery and replay rules.**
- `domain.Rules` holds every rule replay depends on. Changing any of them means bumping `Rules.VERSION`; the
  journal header records it and `CommandJournal.open` refuses a journal written under another version.
- Journal format v3 (`journal.JournalCodec`): `HEADER,3,<rules>,<base>`, then `NEW` / `CANCEL` / `CANCEL_ALL` /
  `SKIP` / `CHECK` text records, each ending in `,<crc32c>\n` (D69). `open` locks `<journal>.lock` (one process at a
  time, D65), verifies the header before changing anything (D64), then cuts a torn last record; a complete
  unparsable or checksum-failing record is never dropped (startup fails). Never let a tool convert it to CRLF.
- Recovery starts from `<journal>.snapshot-<base>` when the header names a base (D71), then replays the journal;
  every `CHECK` digest must match what replay produces (D70), so a behaviour change in code is caught.
  `--compact` (stopped exchange) writes the snapshot and an empty journal; it is also how a `Rules.VERSION`
  change is crossed (compact with the old build). Client order ids are unique per journal (plus resting orders).
- Anything that changes engine outputs changes the digest: bump `Rules.VERSION` for rule changes, and expect a
  DIVERGED replay for old journals after a behaviour change (compact first with the old build).
- Poison pill: if the engine throws a `RuntimeException` on a command live, `Exchange` appends `SKIP <seq>`,
  rebuilds the engine by replaying everything before it, and continues. An `Error` or journal failure halts the
  writer (`Main` exits 2). At startup a failing record is never auto-skipped; the operator runs `--quarantine <seq>`.
- State-dependent rejects (duplicate client order id, price band, open-order and book caps, post-only would
  cross) and self-trade prevention are journalled and replayed; stateless ones (and session limits) never are.

**Gateway security (D74, D75).** Loopback bind by default, `LOGIN <p> <secret> [COD]` against salted PBKDF2
hashes, optional TLS; login/idle timeouts, a per-session in-flight cap and rate limit. Tests build `Credentials`
with few iterations (`Credentials.of(map, 1_000)`); never log a secret.

## Tests

- One test class per production class, `@Nested` groups per behaviour; tests mirror the main packages.
- Every wait in a concurrent test has a timeout; never synchronise with `Thread.sleep`.
- Test seams: `core.CommandInterceptor` (package-private; inject a poison pill or block the writer, see `Gate` in
  `ExchangeServiceTest`), `OrderBook.checkInvariants()` and `ordersAt()` (package-private), `MutableClock` in
  `src/test/java/exchangeserver/testing`.
- On Windows, Java opens files with delete sharing, so deleting a file does **not** prove it was closed; use
  `CommandJournal.openJournals()` to assert a journal handle was released.
- After a behavioural change, run `-Pmutation` and explain survivors in Spec §7 (fsync and log calls are expected
  survivors).
- Expected noise: tests that inject poison pills log a stack trace ("simulated engine bug"); that is not a failure.

## Environment

Windows 11, default charset windows-1252: always pass an explicit charset (the build sets UTF-8). Don't rewrite
source files with Windows PowerShell's `Set-Content -Encoding utf8`: it adds a byte-order mark, which javac
rejects; use the Edit tool or `[IO.File]::WriteAllText(path, text, New-Object Text.UTF8Encoding($false))`. fsync costs
~1.5 ms on the dev disk, and timed waits round up to the ~15.6 ms Windows timer (why the linger batching mode
measured badly). Git repository, branch `main`. Maven compiles incrementally by timestamp: a source restored with
an older modification time (e.g. `Copy-Item` from a backup) is not recompiled and the jar keeps stale classes; use
`mvnw.cmd clean ...` after swapping files.
