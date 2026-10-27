# 02 — Tech Stack

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** every language feature, JDK library and tool this code depends on: what it does here, why it was likely chosen, and the minimum you need to know to read the code | **Prerequisites:** 01 | **Related:** 04, 07-modules/*, 08, 10
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

The stack is deliberately small:

| Layer | What | Version | Scope |
|---|---|---|---|
| Language/runtime | Java (JDK) | 17 (`maven.compiler.release` 17) | everything |
| Build | Apache Maven via the Maven Wrapper | wrapper 3.3.2, Maven 3.9.16 | build |
| Tests | JUnit Jupiter (JUnit 5) | 5.11.4 (BOM) | test |
| Benchmarks | JMH (Java Microbenchmark Harness) | 1.37 | test, profile `bench` |
| Mutation testing | PIT (pitest) + JUnit 5 plugin | 1.17.4 / 1.2.1 | profile `mutation` |

**[Verified]** `pom.xml:13-19`, `pom.xml:33-52`, `pom.xml:132-141`, `.mvn/wrapper/maven-wrapper.properties`.
The JDK used on the dev machine is Temurin 17.0.20.1 (**[Verified]**, `java -version` run on 2026-10-27).

There are **no runtime dependencies**: the shipped jar contains only the project's own classes (about 110 KB,
**[Verified]**). No web framework, no logging library, no database, no JSON. Networking is `java.net` sockets, the
journal is a text file written through `java.nio`, logging is `System.Logger`. The Vision records that Spring Boot
was considered and dropped on 2026-10-24 in favour of a plain TCP gateway (**[Verified]** Vision §8).

**[Inferred]** Why so bare: the project is a learning exercise about durability, determinism and concurrency;
frameworks would hide exactly the mechanisms it wants to make visible (an fsync, a queue hand-off, a thread
boundary). The Vision's principle "Measure before optimizing" and the absence of any third-party runtime code are
consistent with that.

The rest of this guide goes feature by feature. For each: **here** (where and how it is used), **why** (likely
reason), **minimum** (what you must know to read the code).

## 1. Java 17 language features

### 1.1 Records, with compact constructors

**Here.** Almost every immutable value is a `record`: commands (`NewOrderCommand`, `CancelOrderCommand`,
`SkipCommand`), engine outputs (`Response`, `Trade`, `Execution`, `OrderOutput`, `CancelOutput`, `BookSnapshot`),
book results (`Fill`, `LevelUpdate`, `CancelResult`), requests (`Request.Submit`…), parsed client messages, and
`ExchangeService.Settings`. Validation lives in the **compact constructor** (a constructor without a parameter
list that runs before the fields are assigned):

```java
// src/main/java/exchangeserver/command/NewOrderCommand.java:8-20
public record NewOrderCommand(long sequenceNumber, Instant timestamp, long orderId, OrderRequest request)
        implements Command {

    public NewOrderCommand {
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(request, "request");
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Sequence number must be positive");
        }
        if (orderId <= 0) {
            throw new IllegalArgumentException("Order id must be positive");
        }
    }
}
```

Compact constructors are also used to **defensively copy** lists, e.g. `trades = List.copyOf(trades);` in
`Response` (**[Verified]** `src/main/java/exchangeserver/engine/Response.java:21`), so a record cannot be changed
through a list the caller still holds.

**Why.** **[Verified]** Spec §2 invariants: "An invalid `Order`, `OrderRequest`, `Command`, `Response`, `Trade` or
`CancelResult` cannot be constructed." Records give immutability plus generated `equals`/`hashCode`, which the
tests rely on heavily (e.g. comparing whole `BookSnapshot`s or whole output lists for determinism).

**Minimum.** A record's fields are final; accessors are named like the field (`command.sequenceNumber()`, not
`getSequenceNumber()`); `equals` compares all components; in a compact constructor you may reassign the parameter
(`bids = List.copyOf(bids)`) before the implicit field assignment.

Not everything is a record: `Order` (mutable: its filled quantity changes) and `OrderRequest` (private
constructor plus named factories) are ordinary final classes (**[Verified]**
`src/main/java/exchangeserver/domain/Order.java:16`, `src/main/java/exchangeserver/domain/OrderRequest.java:15`).

### 1.2 Sealed interfaces

**Here.** Closed sets of variants:

| Sealed type | Variants | File |
|---|---|---|
| `Command` | `NewOrderCommand`, `CancelOrderCommand`, `SkipCommand` | `src/main/java/exchangeserver/command/Command.java:11` |
| `Request<R>` | `Submit`, `Cancel`, `Snapshot` | `src/main/java/exchangeserver/core/Request.java:16` |
| `EngineOutput` | `OrderOutput`, `CancelOutput` | `src/main/java/exchangeserver/engine/EngineOutput.java:14` |
| `ClientMessage` | `Login`, `NewOrder`, `Cancel`, `Subscribe`, `Unsubscribe`, `Ping`, `Quit` | `src/main/java/exchangeserver/gateway/ClientMessage.java:9` |

```java
// src/main/java/exchangeserver/command/Command.java:11-16
public sealed interface Command permits NewOrderCommand, CancelOrderCommand, SkipCommand {
    long sequenceNumber();
    Instant timestamp();
}
```

**Why.** **[Verified]** the comment above it: "The Java type is the kind tag; the journal writes it as a word"
(`src/main/java/exchangeserver/command/Command.java:7-8`). Sealing documents that no other kind exists.

**Minimum.** `sealed ... permits A, B` means only the listed classes may implement it. In Java 17 the compiler
does **not** yet use this to check `instanceof` chains for exhaustiveness (see 1.4), so the code ends such chains
with a `throw new IllegalStateException("Unknown command type ...")` (**[Verified]**
`src/main/java/exchangeserver/journal/JournalCodec.java:94-96`).

### 1.3 Pattern matching for `instanceof`

**Here.** Everywhere a sealed type is dispatched:

```java
// src/main/java/exchangeserver/core/Exchange.java:168-176
private EngineOutput apply(Command command) {
    if (command instanceof NewOrderCommand n) {
        return engine.onNewOrder(n);
    }
    if (command instanceof CancelOrderCommand c) {
        return engine.onCancel(c);
    }
    throw new IllegalStateException("Not an engine command: " + command);
}
```

**Minimum.** `x instanceof T t` tests the type and binds `t` (already cast) in the true branch. Final in Java 16.

### 1.4 Switch expressions, arrows and `yield`

**Here.** Over enums and strings: the outcome switch in the engine, the journal decoder, the protocol parser and
`Main`'s argument parsing.

```java
// src/main/java/exchangeserver/engine/MatchingEngine.java:155-170
OrderStatus status = switch (order.getOrderType()) {
    case LIMIT, POST_ONLY -> {
        if (remaining > 0) {
            book.add(order);
            yield OrderStatus.RESTED;
        }
        yield OrderStatus.FILLED;
    }
    case IOC, MARKET -> remaining > 0 ? OrderStatus.CANCELLED : OrderStatus.FILLED;
    case FOK -> {
        if (remaining != 0) {
            throw new IllegalStateException("Fillable FOK " + order.getOrderId() + " left " + remaining);
        }
        yield OrderStatus.FILLED;
    }
};
```

**Why.** **[Verified]** Spec D24 and the class comment: "The outcome, as a switch over OrderType with no default
(the compiler rejects a missed type)" (`src/main/java/exchangeserver/engine/MatchingEngine.java:32`). A switch
*expression* over an enum must be exhaustive, so adding a sixth `OrderType` breaks the build here until it is
handled. That is a deliberate compile-time safety net.

**Minimum.** `case A, B -> expr` has no fall-through. A block arm returns its value with `yield`. A switch
*expression* must cover every enum constant (or have `default`).

**Why `instanceof` chains rather than `switch` over sealed types?** **[General knowledge]** Pattern matching for
`switch` (`case NewOrderCommand n ->`) was only a *preview* feature in Java 17 (JEP 406) and became final in Java
21 (JEP 441). With `maven.compiler.release` 17 and no `--enable-preview`, the code cannot use it. **[Inferred]**
This is why dispatch over sealed types uses `instanceof` chains plus a final `throw`, losing the exhaustiveness check
that enum switches get. Guide 12 (change recipes) lists every chain you must update when adding a variant.

### 1.5 Other small features

- `var` is **not** used; types are spelled out (**[Verified]** by search, no `var ` declarations in `src/main`).
- `List.of`, `List.copyOf`, `Set.copyOf` (immutable collections) are used for outputs and defensive copies.
- `Optional` / `OptionalLong`: `OrderBook.bestLevel` returns `Optional<PriceLevel>`; a request's price is an
  `OptionalLong` (empty for MARKET) so the "no price" sentinel `-1` never leaks out of the domain package
  (**[Verified]** `src/main/java/exchangeserver/domain/OrderRequest.java:11-12`, `116-118`; Spec D26).
- Text blocks are not used; class documentation is in `/* ... */` block comments at the top of each file.

## 2. Concurrency (`java.util.concurrent` and the memory model)

You need these to read `core.ExchangeService` and the `gateway` package. Guide 07-modules/exchange-service goes
deeper.

### 2.1 Happens-before, `volatile`, `synchronized` (the minimum)

**[General knowledge]** In Java, a write by one thread is guaranteed visible to another thread only if there is a
**happens-before** edge between them. The edges this code relies on:

| Edge | Where it is used here |
|---|---|
| `Thread.start()`: everything before it is visible to the started thread | The journal is replayed on the caller's thread, then the writer is started: **[Verified]** comment at `src/main/java/exchangeserver/core/ExchangeService.java:117-118` |
| `Thread.join()`: everything the thread did is visible after `join` returns | Stats are read after `stop()` in tests (**[Verified]** `src/test/java/exchangeserver/core/ExchangeServiceTest.java:39-41`) |
| `BlockingQueue` put → take | A `Pending` request needs no `volatile` fields (**[Verified]** `src/main/java/exchangeserver/core/Pending.java:12-13`) |
| `CompletableFuture` complete → `get`/`join`/continuation | Results flow back to callers the same way |
| `volatile` write → read | `ExchangeService.state`, `haltCause`, stats (`src/main/java/exchangeserver/core/ExchangeService.java:98-106`); `ClientSession.participant` (`src/main/java/exchangeserver/gateway/ClientSession.java:55`) |
| `synchronized` unlock → lock of the same monitor | `ExchangeService`'s hand-in lock (`src/main/java/exchangeserver/core/ExchangeService.java:187-197`); `ClientSession.subscriptionLock` (`src/main/java/exchangeserver/gateway/ClientSession.java:49`) |

**Minimum.** `volatile` gives visibility of single reads/writes, not atomicity of `x++` (fine here because only the
writer thread writes the stats). `synchronized (lock) { ... }` gives mutual exclusion plus visibility.

### 2.2 `BlockingQueue`: `LinkedBlockingQueue`, `ArrayBlockingQueue`, `drainTo`

**Here.**
- The exchange's single request queue is an **unbounded** `LinkedBlockingQueue`; its capacity (100,000) is
  enforced by hand under the lock, so `stop()` can always add its STOP marker without blocking (**[Verified]**
  `src/main/java/exchangeserver/core/ExchangeService.java:95`, `192`; Spec D56).
- The writer takes one request (blocking) and then `drainTo(batch, max - 1)` takes whatever else is already
  waiting without blocking. That pair *is* smart batching:

```java
// src/main/java/exchangeserver/core/ExchangeService.java:252-254
batch.clear();
batch.add(queue.take());
queue.drainTo(batch, settings.maxBatchSize() - 1);
```

- Each client session's outbound queue is a **bounded** `ArrayBlockingQueue` (10,000 lines); producers call
  non-blocking `offer`, which returns `false` when full (**[Verified]**
  `src/main/java/exchangeserver/gateway/ClientSession.java:61`, `82`).

**Minimum.** `put`/`take` block; `offer`/`poll` don't (or wait with a timeout); `drainTo` moves up to *n* waiting
elements in one call.

### 2.3 `CompletableFuture`

**Here.** Each request carries a `CompletableFuture` (in `Pending.result`) that the writer completes. Two
subtleties matter:

1. **Callbacks attached with `whenComplete` run on the completing thread** if the future is not yet done when
   they are attached. The gateway's reply callbacks are attached *before* the request is queued, so they run on the
   `exchange-writer` thread, before the listener (**[Verified]**
   `src/main/java/exchangeserver/core/ExchangeService.java:166-170`, `183-185`). If the request is refused at once,
   the callback runs on the caller's thread instead.
2. **The futures returned to ordinary callers are separate futures**, completed asynchronously on the
   **common ForkJoinPool** (the JVM-wide shared thread pool used by `*Async` methods without an executor):

```java
// src/main/java/exchangeserver/core/ExchangeService.java:154-164
private static <R> CompletableFuture<R> handOff(CompletableFuture<R> internal) {
    CompletableFuture<R> mine = new CompletableFuture<>();
    internal.whenCompleteAsync((value, failure) -> {
        if (failure != null) {
            mine.completeExceptionally(failure);
        } else {
            mine.complete(value);
        }
    });
    return mine;
}
```

**Why.** **[Verified]** Spec D61: with a shared future, a caller's `cancel()` or `orTimeout()` made the writer skip a
command it had already journalled (so the live book differed from the replayed one), and a caller's blocking
continuation ran on the writer thread and stalled the exchange.

**Minimum.** `complete`/`completeExceptionally` the first time wins; later calls are no-ops (that is why
`Pending.fail` is safe to call on an already-completed request, `src/main/java/exchangeserver/core/Pending.java:32-35`).
`whenComplete` vs `whenCompleteAsync`: same thread vs a pool thread. An exception thrown *inside* a `whenComplete`
callback is captured in the derived future and otherwise silently ignored.

### 2.4 Concurrent collections and atomics

- `ConcurrentHashMap.newKeySet()` for the gateway's `sessions` and `subscribers` sets; `ConcurrentHashMap.compute`
  / `computeIfPresent` to add/remove a session to/from its participant's set atomically, dropping the entry when it
  empties (**[Verified]** `src/main/java/exchangeserver/gateway/TcpGateway.java:136-165`; Spec §9 finding 6).
  `compute` runs the function atomically per key, so an add cannot slip into a set that a concurrent remove is
  dropping.
- `AtomicBoolean closed` in `ClientSession` (`compareAndSet` makes `close()` idempotent), `AtomicLong` session ids,
  `AtomicInteger OPEN` counting open journals for tests (**[Verified]**
  `src/main/java/exchangeserver/gateway/ClientSession.java:48`, `src/main/java/exchangeserver/journal/CommandJournal.java:50`).

### 2.5 Threads

Plain `java.lang.Thread`s with names, no executors (except the common pool used by `whenCompleteAsync`). The
writer is **non-daemon** (the JVM must not exit with accepted requests unprocessed); the acceptor and session
threads are **daemon** (they must not keep the JVM alive) (**[Verified]**
`src/main/java/exchangeserver/core/ExchangeService.java:112-114`, `src/main/java/exchangeserver/gateway/TcpGateway.java:79-80`,
`src/main/java/exchangeserver/gateway/ClientSession.java:62-65`).

## 3. Files and durability (`java.nio`)

### 3.1 `FileChannel`, `force`, `truncate`

**Here.** The journal is opened with `FileChannel.open(file, CREATE, WRITE, APPEND)`: `APPEND` means every write
goes to the current end of file, so history cannot be overwritten (**[Verified]**
`src/main/java/exchangeserver/journal/CommandJournal.java:79`). After writing a batch, `channel.force(true)` asks
the OS to flush the data (and, with `true`, file metadata such as size) to the storage device: this is the
**fsync** (**[Verified]** `src/main/java/exchangeserver/journal/CommandJournal.java:131`). Recovery opens a second
channel and `truncate`s the file to just after the last `\n` (**[Verified]**
`src/main/java/exchangeserver/journal/CommandJournal.java:272-282`).

**Minimum.** `write` returning does not mean the data is on disk; only `force` does. One `force` cost about
1.5 ms on the author's disk (**[Verified]** Spec §5), which is why batching matters. Reading uses separate
`InputStream`s (`Files.newInputStream`), so replay can read while the append channel is open.

### 3.2 `FileLock` and the `<journal>.lock` file (added 2026-10-27, D65)

**Here.** To stop two processes (e.g. a running exchange and an operator's `--quarantine` run) from appending to
the same journal, `CommandJournal.open` first takes an exclusive lock on a **separate, empty file next to the
journal**, `<journal>.lock` (e.g. `exchange.journal.lock`), before it opens the journal at all:

```java
// src/main/java/exchangeserver/journal/CommandJournal.java:207-238 (trimmed)
private record Lock(Path key, FileChannel channel) {
    static Lock acquire(Path journal) throws IOException {
        Path lockFile = journal.resolveSibling(journal.getFileName() + ".lock");
        try {
            Files.createFile(lockFile);     // create-new: never opens, so never closes, an existing lock file
        } catch (FileAlreadyExistsException normalAfterTheFirstRun) {
            // reuse it
        }
        Path key = lockFile.toRealPath();   // one key however the journal path is spelled
        if (!HELD.add(key)) {               // static set: a second open in this JVM is refused here
            throw alreadyOpen(journal);
        }
        FileChannel channel = FileChannel.open(lockFile, WRITE);
        if (channel.tryLock() == null) {    // another process holds it
            throw alreadyOpen(journal);     // (the real code also closes the channel and leaves HELD)
        }
        return new Lock(key, channel);
    }
}
```

`close()` closes the journal channel, then the lock channel (which releases the OS lock), then removes the key
from `HELD`; the empty lock file stays on disk (**[Verified]**
`src/main/java/exchangeserver/journal/CommandJournal.java:187-196`, `240-247`; `.gitignore` ignores
`*.journal.lock`).

**Why a separate file: OS lock semantics differ.** **[Verified]** the design comment at
`src/main/java/exchangeserver/journal/CommandJournal.java:200-206` and Spec D65:

| | Windows | Linux / macOS (POSIX `fcntl` locks) |
|---|---|---|
| Strength | **Mandatory**: a locked range cannot be read or written through any *other* handle, even in the same process | **Advisory**: only stops others who also ask for the lock |
| Owner | The handle (channel) that took it | The **whole process** |
| Released when | That handle closes | **Any** handle the process has on that file closes |

The journal itself is opened and closed constantly: the header check, the torn-tail cut and every `read()`
(replay, the poison-pill rebuild) use their own short-lived handles. Under POSIX semantics, the first of those
closes would silently drop a lock held on the journal. Under Windows semantics, a lock on the journal's *data*
would block those reads. A lock file that **nothing else ever opens or closes** avoids both. That is also why the
lock file is created with `Files.createFile` (create-new, which fails without opening an existing file) rather
than opened-if-exists, and why a second open **in the same JVM** is refused by the in-memory `HELD` set before it
touches the lock file: opening and closing a second channel on it would, on POSIX, drop the first holder's lock.

History: the first version of D65 locked one byte at `Long.MAX_VALUE - 1` *of the journal itself* (past the data,
to dodge Windows' mandatory locks). It passed on Windows, where locks belong to a handle, but would have vanished on
Linux/macOS at the first `read()`; it was replaced by the lock file the same day (**[Verified]** Spec D65).

**Minimum.** `FileChannel.tryLock()` returns `null` if **another process** holds an overlapping lock, and throws
`OverlappingFileLockException` if **this JVM** already holds one on that file through another channel (the
`FileLock` Javadoc also warns that, on some systems, closing a channel releases all of the JVM's locks on the file).
A lock is released when its channel closes. **[Verified]** cross-process refusal is tested with a child JVM
(`CommandJournalTest.Exclusive.anotherProcessIsRefusedWhileTheJournalIsOpen`, which first calls `read()` so that a
lock on the journal itself would have been dropped on POSIX); it has run on Windows only, not yet on Linux/macOS
(Spec D65).

### 3.3 Charsets

Every byte/char conversion names a charset: `UTF_8` for the journal, `US_ASCII` for the wire protocol
(**[Verified]** e.g. `src/main/java/exchangeserver/journal/CommandJournal.java:21`,
`src/main/java/exchangeserver/gateway/ClientSession.java:22`). **Why:** **[Verified]** `CLAUDE.md`: the dev
machine's default charset is windows-1252, so relying on the default would make the code behave differently across
machines.

## 4. Networking (`java.net`)

**Here.** Blocking, thread-per-connection sockets: a `ServerSocket` bound to `new InetSocketAddress(port)` (all
interfaces), `SO_REUSEADDR` on, `TCP_NODELAY` on each accepted socket (disables Nagle's algorithm, which would
otherwise delay small writes to batch them) (**[Verified]**
`src/main/java/exchangeserver/gateway/TcpGateway.java:65-67`, `108`). Each session has a reader thread doing
byte-by-byte `read()` from a `BufferedInputStream`, and a sender thread writing through a `BufferedWriter`, flushing
once per burst.

**Why.** **[Verified]** Spec §6 and Vision §5: "plain TCP, line protocol, thread-per-connection". **[Inferred]**
Blocking I/O with two threads per client is the simplest model to reason about; NIO or virtual threads are listed as
a future candidate (Vision §7, "NIO or virtual-thread gateway (Java 21)").

**Minimum.** Closing a socket's input or output stream closes the whole socket (which is why the reader never uses
try-with-resources on the input stream: **[Verified]** `src/main/java/exchangeserver/gateway/ClientSession.java:115-116`).
A blocked `read()` is woken by closing the socket from another thread.

## 5. Time: `java.time.Clock` injection

**Here.** The `Sequencer` takes a `Clock` and is the only class that reads it (**[Verified]**
`src/main/java/exchangeserver/command/Sequencer.java:22`, `74-81`). Production passes `Clock.systemUTC()`
(`src/main/java/exchangeserver/core/ExchangeService.java:69`); tests pass a `MutableClock` they can move, even
backwards (**[Verified]** `src/test/java/exchangeserver/testing/MutableClock.java`). Timestamps are `Instant`s,
written to the journal with `Instant.toString()` (ISO-8601, nanosecond precision).

**Why.** **[Verified]** Spec D32/D33: the sequencer owns all stamps, and a clock that steps backwards reuses the
previous timestamp; an injected clock makes that testable.

## 6. Logging: `System.Logger`

**Here.** Three classes log, all through `System.getLogger(...)`: `Exchange`, `ExchangeService`, `TcpGateway`
(**[Verified]** `src/main/java/exchangeserver/core/Exchange.java:51`,
`src/main/java/exchangeserver/core/ExchangeService.java:89`, `src/main/java/exchangeserver/gateway/TcpGateway.java:45`).
Everything else communicates through return values and exceptions; `Main` prints with `System.out`/`System.err`.

**Minimum.** **[General knowledge]** `System.Logger` (Java 9+) is a logging facade built into the JDK. With no
logging library on the classpath it routes to `java.util.logging`, whose default console handler prints `INFO` and
above to **stderr** in a two-line format (`<date> <class> <method>` then `LEVEL: message`). You saw that format in
the test output: `SEVERE: Exchange halted` followed by a stack trace (**[Verified]**, observed during the test run;
`ERROR` maps to `SEVERE`).

## 7. Build: Maven and the Maven Wrapper

**Here.** `pom.xml` is the only build file. The **Maven Wrapper** (`mvnw`, `mvnw.cmd`,
`.mvn/wrapper/maven-wrapper.properties`) downloads Maven 3.9.16 on first use, so no local Maven install is needed
(**[Verified]** `CLAUDE.md`, README). Key settings (**[Verified]** `pom.xml`):

- `maven.compiler.release` 17 and UTF-8 source encoding (`pom.xml:14-16`).
- `-Xlint:all` minus `processing` (`pom.xml:62-65`); the project rule is "keep the build warning-free"
  (`CLAUDE.md`).
- Surefire (the test runner plugin) **excludes** `exchangeserver/bench/**` so benchmarks never run as tests
  (`pom.xml:68-77`).
- The jar manifest's `Main-Class` is `exchangeserver.Main`, so `java -jar` works (`pom.xml:78-89`).
- Profile `bench`: runs JMH via `exec-maven-plugin` in the `verify` phase, with `skipTests`; options come from
  `-Djmh.args="..."` (`pom.xml:95-125`).
- Profile `mutation`: runs PIT in the `test` phase on packages `domain`, `book`, `engine`, `command`, `journal`,
  `core` (not `gateway`, `client`, `Main`) with 4 threads (`pom.xml:127-169`).

**Minimum.** A **profile** is a named block of extra build configuration switched on with `-P<id>`. Maven
compiles **incrementally by file timestamp**: a source file restored with an *older* modification time than its
`.class` file is not recompiled. **[Verified]** this happened during the 2026-10-27 survey (a jar silently kept old
classes); `CLAUDE.md` now says to use `clean` after swapping files.

## 8. Testing: JUnit 5 (Jupiter)

**Here.** Every test class uses these features (**[Verified]** across `src/test/java`):

| Feature | Meaning | Example |
|---|---|---|
| `@Test` | a test method | everywhere |
| `@Nested` | an inner (non-static) class grouping tests by behaviour; shares the outer instance's fields | `MatchingEngineTest.Limit`, `.Ioc`, `.Fok`… |
| `@TempDir Path dir` | JUnit creates and deletes a temporary directory per test | journals in `ExchangeTest`, `CommandJournalTest` |
| `@ParameterizedTest` + `@ValueSource`/`@CsvSource` | one test body, many inputs | `OrderRequestTest.Validation`, `JournalCodecTest.Corrupt` |
| `@RepeatedTest(5)` | run the same test 5 times | `ExchangeServiceTest.Stop.racingStopNeverLosesARequest` |
| `@BeforeEach` / `@AfterEach` | setup / teardown | `TcpGatewayTest` starts a gateway on port 0 |
| `assertThrows`, `assertInstanceOf` | exceptions and types | widely |

The project convention: **one test class per production class, `@Nested` groups per behaviour; every wait in a
concurrent test has a timeout** (**[Verified]** `CLAUDE.md`, Spec §1.2). Guide 10 covers the strategy.

## 9. JMH (benchmarks)

**Here.** Two benchmarks in `src/test/java/exchangeserver/bench`: `MatchingBenchmark` (engine cost per command at
10 / 1,000 / 100,000 price levels) and `BatchingBenchmark` (throughput and latency of fsync-per-command vs linger vs
smart batching with 1/16/64 client threads). The JMH annotation processor generates the harness code at test-compile
time (`jmh-generator-annprocess`, `pom.xml:46-51`).

**Why.** **[Verified]** Vision principle 7 "Measure before optimizing" and Spec §5: the results decided D59 (no cached
best-level pointer) and D52 (smart batching).

**Minimum.** `@State` holds per-thread or per-benchmark state, `@Setup(Level.Iteration)` rebuilds it, `@Param`
runs the benchmark once per listed value, and the return value of a `@Benchmark` method is consumed so the JIT cannot
delete the work. **[Verified]** not run during the 2026-10-27 survey; the numbers in guides come from Spec §5.

## 10. PIT (mutation testing)

**Here.** `mvnw.cmd -Pmutation test` makes thousands of small changes ("mutants") to the bytecode, e.g. `<` to
`<=`, removing a method call, returning 0, and reruns the tests against each. A mutant the tests catch is **killed**;
one they don't is a **survivor**, i.e. a behaviour no test pins down. Report: `target/pit-reports/index.html`
(**[Verified]** README, `pom.xml:127-169`). Last recorded result before the D63–D65 fixes: 478 mutations, 444 killed
(93%), with each surviving kind explained (**[Verified]** Spec §7).

**Why.** **[Verified]** pom comment: "the spec's 'break the code on purpose', automated" (`pom.xml:126`). The
project rule is to run it after any behavioural change and explain survivors (`CLAUDE.md`).

## Key takeaways

- Java 17 with zero runtime dependencies; Maven Wrapper; JUnit 5; JMH and PIT only in opt-in profiles.
- Records with validating compact constructors make invalid values unconstructable; sealed interfaces close the set
  of commands, requests, outputs and messages.
- Enum `switch` expressions are exhaustive by compiler; sealed-type dispatch uses `instanceof` chains ending in a
  `throw` because pattern `switch` is not final in Java 17.
- The concurrency toolkit: one `LinkedBlockingQueue` (+ `drainTo` = smart batching), per-session
  `ArrayBlockingQueue` with `offer`, `CompletableFuture`s (caller futures handed off via the common pool, D61),
  `volatile`/`synchronized` for the few shared fields, `ConcurrentHashMap.compute` in the gateway.
- Durability = `FileChannel` in `APPEND` mode + `force(true)` per batch; recovery uses `truncate`; an exclusive `FileLock`
  on a separate `<journal>.lock` file (plus an in-JVM set) keeps a second opener out, because POSIX locks vanish when
  any handle on the locked file closes and Windows locks block other handles.
- All charsets are explicit; the clock is injected; logging is `System.Logger` → `java.util.logging` on stderr.
- Maven compiles by timestamp: after swapping files, build with `clean`.

## Common confusions

- **"`whenComplete` runs on a pool thread."** Only `whenCompleteAsync` does. `whenComplete` runs on whichever
  thread completes the future (here: the writer), or immediately on the caller if already complete.
- **"`write` to a `FileChannel` makes data durable."** No: only `force` does. A crash between `write` and `force`
  may lose the data, or may keep part of it (hence torn-tail recovery).
- **"The journal file is locked."** The lock is on `<journal>.lock`, an empty sibling file; the journal itself is
  never locked. It is a "this journal is in use" flag, not a lock on the records. The lock file staying on disk after
  a stop is normal.
- **"A Java `FileLock` behaves the same everywhere."** No: on Linux/macOS it is per process and is dropped when *any*
  handle on that file closes; on Windows it is per handle and mandatory.
- **"`sealed` makes `instanceof` chains exhaustive."** Not in Java 17. Only enum switch expressions get that check
  here.
- **"Benchmarks run with `mvnw test`."** They are excluded from Surefire and run only with `-Pbench verify`.

## Questions to test yourself

1. Why is the exchange's request queue unbounded when the exchange enforces a capacity of 100,000?
   <details><summary>Answer</summary>So `stop()` can always add its STOP marker without blocking on a full queue;
   the capacity is enforced by checking `queue.size()` under the same lock as the state check, and a full queue
   refuses with BUSY (Spec D56).</details>
2. A gateway callback and a caller's `thenAccept` are both attached to request results. On which threads do they
   run?
   <details><summary>Answer</summary>The gateway callback is attached with `whenComplete` to the internal future
   before enqueueing, so it runs on the `exchange-writer` thread (or on the caller's thread if refused at once). A
   caller's `thenAccept` is attached to its own handed-off future, completed by `whenCompleteAsync` on the common
   ForkJoinPool, so it runs off the writer.</details>
3. Why does `CommandJournal` lock a separate `<journal>.lock` file instead of the journal itself? Give the Linux
   reason and the Windows reason.
   <details><summary>Answer</summary>Linux/macOS: POSIX locks belong to the process and are dropped when any handle
   on the file closes; the header check, the torn-tail cut and every `read()` open and close handles on the journal,
   so a lock on it would vanish almost at once. Windows: locks are mandatory, so a lock on the journal's records would
   block those same reads and the truncate. A lock file that nothing else opens or closes has neither problem.</details>
4. Why does `JournalCodec.encode` end with `throw new IllegalStateException("Unknown command type ...")` even
   though `Command` is sealed?
   <details><summary>Answer</summary>Java 17 does not check `instanceof` chains for exhaustiveness, and pattern
   matching for `switch` is only a preview there. The throw turns a forgotten variant into a loud failure instead of
   silently writing nothing.</details>
5. You replaced `CommandJournal.java` with an older backup and ran `mvnw.cmd package`. The jar still behaves like
   the newer code. Why, and what is the fix?
   <details><summary>Answer</summary>Maven's incremental compiler compares timestamps; the restored source is older
   than its `.class` file, so it is not recompiled. Run `mvnw.cmd clean package`.</details>
6. Which packages does PIT mutate, and which are only covered by end-to-end tests?
   <details><summary>Answer</summary>Mutated: `domain`, `book`, `engine`, `command`, `journal`, `core`. Not
   mutated: `gateway`, `client`, `Main` (the gateway is covered by end-to-end socket tests, Spec §7).</details>
