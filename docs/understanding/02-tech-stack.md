# 02 — Tech Stack

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** every language feature, JDK library and tool this code depends on: what it does here, why it was likely chosen, and the minimum you need to know to read the code | **Prerequisites:** 01 | **Related:** 04, 07-modules/*, 08, 10
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

The stack is deliberately small:

| Layer | What | Version | Scope |
|---|---|---|---|
| Language/runtime | Java (JDK) | 17 (`maven.compiler.release` 17) | everything |
| Build | Apache Maven via the Maven Wrapper | wrapper 3.3.2, Maven 3.9.16 | build |
| Tests | JUnit Jupiter (JUnit 5) | 5.11.4 (BOM) | test |
| Benchmarks | JMH (Java Microbenchmark Harness) | 1.37 | test, profile `bench` |
| Mutation testing | PIT (pitest) + JUnit 5 plugin | 1.17.4 / 1.2.1 | profile `mutation` |
| Linux runs | Docker image `eclipse-temurin:17` | — | manual check, not part of the build |

**[Verified]** `pom.xml:13-19`, `pom.xml:33-52`, `pom.xml:132-141`, `.mvn/wrapper/maven-wrapper.properties`.
The JDK used on the dev machine is Temurin 17.0.20.1, and the Linux container ran the same version
(**[Verified — observed]**, `java -version` on 2026-10-27).

There are **no runtime dependencies**: the shipped jar contains only the project's own classes. No web framework,
no logging library, no database, no JSON, no crypto library. Networking is `java.net` sockets (and `javax.net.ssl`
for TLS), the journal is a text file written through `java.nio`, password hashing uses the JDK's
`SecretKeyFactory`, logging is `System.Logger`. The Vision records that Spring Boot was considered and dropped on
2026-10-24 in favour of a plain TCP gateway (**[Verified]** Vision §8).

**[Inferred]** Why so bare: the project is a learning exercise about durability, determinism and concurrency;
frameworks would hide exactly the mechanisms it wants to make visible (an fsync, a queue hand-off, a thread
boundary, a checksum). The Vision's principle "Measure before optimizing" and the absence of any third-party
runtime code are consistent with that.

The rest of this guide goes feature by feature. For each: **here** (where and how it is used), **why** (likely
reason), **minimum** (what you must know to read the code).

## 1. Java 17 language features

### 1.1 Records, with compact constructors

**Here.** Almost every immutable value is a `record`: commands (`NewOrderCommand`, `CancelOrderCommand`,
`CancelAllCommand`, `SkipCommand`, `CheckCommand`), engine outputs (`Response`, `Trade`, `Execution`, `OrderOutput`,
`CancelOutput`, `CancelAllOutput`, `BookSnapshot`, `EngineState`), book results (`Fill`, `LevelUpdate`,
`CancelResult`, `RestingOrderView`), private events (`PrivateEvent.OrderReply` …), requests (`Request.Submit` …),
parsed client messages, `Snapshot`, and settings (`ExchangeService.Settings`, `TcpGateway.Settings`,
`Exchange.Options`). Validation lives in the **compact constructor** (a constructor without a parameter list that
runs before the fields are assigned):

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

A record may also override a generated method. `ClientMessage.Login` overrides `toString()` so the secret never
appears if a parsed message is logged (**[Verified]** `src/main/java/exchangeserver/gateway/ClientMessage.java:11-17`).

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
| `Command` | `NewOrderCommand`, `CancelOrderCommand`, `CancelAllCommand`, `SkipCommand`, `CheckCommand` | `src/main/java/exchangeserver/command/Command.java:11` |
| `Request<R>` | `Submit`, `Cancel`, `CancelAll`, `Snapshot`, `Attach`, `Resend` | `src/main/java/exchangeserver/core/Request.java:22` |
| `EngineOutput` | `OrderOutput`, `CancelOutput`, `CancelAllOutput` | `src/main/java/exchangeserver/engine/EngineOutput.java:14` |
| `PrivateEvent` | `OrderReply`, `CancelReply`, `MakerFill` | `src/main/java/exchangeserver/core/PrivateEvent.java:15` |
| `ClientMessage` | `Login`, `NewOrder`, `Cancel`, `Resend`, `Subscribe`, `Unsubscribe`, `Ping`, `Quit` | `src/main/java/exchangeserver/gateway/ClientMessage.java:9` |

```java
// src/main/java/exchangeserver/command/Command.java:11-16
public sealed interface Command permits NewOrderCommand, CancelOrderCommand, CancelAllCommand, SkipCommand, CheckCommand {
    long sequenceNumber();
    Instant timestamp();
}
```

**Why.** **[Verified]** the comment above it: "The Java type is the kind tag; the journal writes it as a word"
(`src/main/java/exchangeserver/command/Command.java:8`). Sealing documents that no other kind exists.

**Minimum.** `sealed ... permits A, B` means only the listed classes may implement it. In Java 17 the compiler
does **not** yet use this to check `instanceof` chains for exhaustiveness (see 1.4), so the code ends such chains
with a `throw` (**[Verified]** `src/main/java/exchangeserver/journal/JournalCodec.java:124`,
`src/main/java/exchangeserver/core/Ledger.java:51`).

### 1.3 Pattern matching for `instanceof`

**Here.** Everywhere a sealed type is dispatched:

```java
// src/main/java/exchangeserver/core/Ledger.java:42-55
Applied apply(Command command) {
    EngineOutput output;
    if (command instanceof NewOrderCommand n) {
        output = engine.onNewOrder(n);
    } else if (command instanceof CancelOrderCommand c) {
        output = engine.onCancel(c);
    } else if (command instanceof CancelAllCommand c) {
        output = engine.onCancelAll(c);
    } else {
        throw new IllegalStateException("Not an engine command: " + command);
    }
    digest.update(output);
    return new Applied(output, log.record(command, output));
}
```

**Minimum.** `x instanceof T t` tests the type and binds `t` (already cast) in the true branch. Final in Java 16.

### 1.4 Switch expressions, arrows and `yield`

**Here.** Over enums and strings: the outcome switch in the engine, the journal decoder, the protocol parser and
`Main`'s argument parsing.

```java
// src/main/java/exchangeserver/engine/MatchingEngine.java:242-260
OrderStatus status = switch (type) {
    case LIMIT, POST_ONLY -> {
        if (remaining == 0) {
            yield OrderStatus.FILLED;
        }
        if (selfTradePrevented) {
            yield OrderStatus.CANCELLED;
        }
        book.add(order);
        yield OrderStatus.RESTED;
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
(the compiler rejects a missed type)" (`src/main/java/exchangeserver/engine/MatchingEngine.java:36`). A switch
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

- `var` appears only once, in `Ledger.snapshot` (`var state = engine.exportState();`); elsewhere types are spelled
  out (**[Verified]** by search in `src/main`).
- `List.of`, `List.copyOf`, `Set.copyOf`, `Map.copyOf` (immutable collections) are used for outputs and defensive
  copies.
- `Optional` / `OptionalLong`: `OrderBook.bestLevel` returns `Optional<PriceLevel>`; a request's price is an
  `OptionalLong` (empty for MARKET) so the "no price" sentinel `-1` never leaks out of the domain package
  (**[Verified]** `src/main/java/exchangeserver/domain/OrderRequest.java:11-12`, `116`; Spec D26).
- `java.util.function.Predicate` as a visitor: `OrderBook.walk(side, visitor)` stops when the visitor returns
  `false` (**[Verified]** used by the engine's fillability check, cancel-all and snapshot export).
- Text blocks are not used; class documentation is in `/* ... */` block comments at the top of each file.

## 2. Concurrency (`java.util.concurrent` and the memory model)

You need these to read `core.ExchangeService` and the `gateway` package. Guide 07-modules/exchange-service goes
deeper.

### 2.1 Happens-before, `volatile`, `synchronized` (the minimum)

**[General knowledge]** In Java, a write by one thread is guaranteed visible to another thread only if there is a
**happens-before** edge between them. The edges this code relies on:

| Edge | Where it is used here |
|---|---|
| `Thread.start()`: everything before it is visible to the started thread | The journal is replayed on the caller's thread, then the writer is started: **[Verified]** comment at `src/main/java/exchangeserver/core/ExchangeService.java:124-125` |
| `Thread.join()`: everything the thread did is visible after `join` returns | Stats are read after `stop()` in tests (**[Verified]** `src/test/java/exchangeserver/core/ExchangeServiceTest.java:40-41`) |
| `BlockingQueue` put → take | A `Pending` request needs no `volatile` fields (**[Verified]** `src/main/java/exchangeserver/core/Pending.java:12-13`) |
| `CompletableFuture` complete → `get`/`join`/continuation | Results flow back to callers the same way |
| `volatile` write → read | `ExchangeService.state`, `haltCause`, stats (`src/main/java/exchangeserver/core/ExchangeService.java:104-112`); `ClientSession.participant` (`src/main/java/exchangeserver/gateway/ClientSession.java:70`) |
| `synchronized` unlock → lock of the same monitor | `ExchangeService`'s hand-in lock (`src/main/java/exchangeserver/core/ExchangeService.java:223`); `ClientSession.subscriptionLock` (`src/main/java/exchangeserver/gateway/ClientSession.java:64`) |
| Atomic variables | `ClientSession.inFlight` (`AtomicInteger`) counts requests in flight across the reader thread and the writer's callbacks (`src/main/java/exchangeserver/gateway/ClientSession.java:74`, Spec D76) |

**Minimum.** `volatile` gives visibility of single reads/writes, not atomicity of `x++` (fine here because only the
writer thread writes the stats). `synchronized (lock) { ... }` gives mutual exclusion plus visibility. An
`AtomicInteger`'s `incrementAndGet`/`decrementAndGet` are atomic and visible across threads.

### 2.2 `BlockingQueue`: `LinkedBlockingQueue`, `ArrayBlockingQueue`, `drainTo`

**Here.**
- The exchange's single request queue is an **unbounded** `LinkedBlockingQueue`; its capacity (100,000) is
  enforced by hand under the lock, so `stop()` can always add its stop marker without blocking (**[Verified]**
  `src/main/java/exchangeserver/core/ExchangeService.java:101`, `228`; Spec D56).
- The writer takes one request (blocking) and then `drainTo(batch, max - 1)` takes whatever else is already
  waiting without blocking. That pair *is* smart batching:

```java
// src/main/java/exchangeserver/core/ExchangeService.java:292-294
batch.clear();
batch.add(queue.take());
queue.drainTo(batch, settings.maxBatchSize() - 1);
```

- Each client session's outbound queue is a **bounded** `ArrayBlockingQueue` (10,000 lines); producers call
  non-blocking `offer`, which returns `false` when full (**[Verified]**
  `src/main/java/exchangeserver/gateway/ClientSession.java:89`, `120`).

**Minimum.** `put`/`take` block; `offer`/`poll` don't (or wait with a timeout); `drainTo` moves up to *n* waiting
elements in one call.

### 2.3 `CompletableFuture`

**Here.** Each request carries a `CompletableFuture` (in `Pending.result`) that the writer completes. Three
subtleties matter:

1. **Callbacks attached with `whenComplete` run on the completing thread** if the future is not yet done when
   they are attached. The gateway's callbacks are attached *before* the request is queued, so they run on the
   `exchange-writer` thread, **after** the listener for that command (Spec D76: `Exchange.process` calls the
   listener, then completes the future). If the request is refused at once, the callback runs on the caller's
   thread instead (**[Verified]** `src/main/java/exchangeserver/core/ExchangeService.java:182-185`, `211-221`).
2. **A callback that throws would vanish**, because `whenComplete` captures the exception in a derived future
   nobody reads. The service wraps each callback and logs a throw as `Request callback failed` (Spec D77, M9):

```java
// src/main/java/exchangeserver/core/ExchangeService.java:212-221
if (onDone != null) {
    pending.result.whenComplete((value, failure) -> {
        // whenComplete would capture a throw in a future nobody reads: log it instead (M9).
        try {
            onDone.accept(value, failure);
        } catch (RuntimeException | Error callbackBug) {
            LOG.log(Level.ERROR, "Request callback failed", callbackBug);
        }
    });
}
```

3. **The futures returned to ordinary callers are separate futures**, completed asynchronously on the
   **common ForkJoinPool** (the JVM-wide shared thread pool used by `*Async` methods without an executor):

```java
// src/main/java/exchangeserver/core/ExchangeService.java:170-180
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
`whenComplete` vs `whenCompleteAsync`: same thread vs a pool thread.

### 2.4 Concurrent collections and atomics

- `ConcurrentHashMap.newKeySet()` for the gateway's `sessions` and `subscribers` sets; `ConcurrentHashMap.compute`
  / `computeIfPresent` to add/remove a session to/from its participant's set atomically, dropping the entry when it
  empties (**[Verified]** `src/main/java/exchangeserver/gateway/TcpGateway.java:107-108`, `223-255`; Spec §9 finding
  6). `compute` runs the function atomically per key, so an add cannot slip into a set that a concurrent remove is
  dropping. The same `computeIfPresent` tells `removed()` whether the closing session was the participant's last,
  which decides cancel-on-disconnect (Spec D72).
- `JournalLock.HELD` is a static `ConcurrentHashMap.newKeySet()` of lock-file paths held in this JVM
  (`src/main/java/exchangeserver/journal/JournalLock.java:27`).
- `AtomicBoolean closed` in `ClientSession` (`compareAndSet` makes `close()` idempotent), `AtomicLong` session ids,
  `AtomicInteger OPEN` counting open journals for tests (**[Verified]**
  `src/main/java/exchangeserver/gateway/ClientSession.java:63`, `src/main/java/exchangeserver/gateway/TcpGateway.java:106`,
  `src/main/java/exchangeserver/journal/CommandJournal.java:50`).

### 2.5 Threads

Plain `java.lang.Thread`s with names, no executors (except the common pool used by `whenCompleteAsync`). The
writer is **non-daemon** (the JVM must not exit with accepted requests unprocessed); the acceptor and session
threads are **daemon** (they must not keep the JVM alive) (**[Verified]**
`src/main/java/exchangeserver/core/ExchangeService.java:119-121`, `src/main/java/exchangeserver/gateway/TcpGateway.java:144-145`,
`src/main/java/exchangeserver/gateway/ClientSession.java:91-94`).

### 2.6 `Runtime.halt` in a shutdown hook

**Here.** `Main` registers a shutdown hook (a thread the JVM runs when it is asked to exit, including on Ctrl+C or
SIGTERM). The hook closes the gateway, stops the service, then ends the process itself with
`Runtime.getRuntime().halt(status)`: 0 normally, 1 if the writer did not stop in time, 2 if it had halted
(**[Verified]** `src/main/java/exchangeserver/Main.java:159-177`).

**Why.** **[General knowledge]** When the JVM shuts down because of a signal, its exit status is normally derived
from the signal (130 for SIGINT, 143 for SIGTERM on Linux), not from `main` returning. `halt` sets the status
explicitly and skips any remaining hooks. **[Verified — observed]** In Docker on Linux, the server exited with 0
after SIGINT and after SIGTERM (Spec D77).

**Minimum.** `System.exit` runs shutdown hooks; `Runtime.halt` does not and stops at once; calling `halt` as the
hook's last act is therefore safe only because there is nothing left to do.

## 3. Files and durability (`java.nio`, `java.util.zip`)

### 3.1 `FileChannel`, `force`, `truncate`, atomic moves

**Here.** The journal is opened with `FileChannel.open(file, CREATE, WRITE, APPEND)`: `APPEND` means every write
goes to the current end of file, so history cannot be overwritten (**[Verified]**
`src/main/java/exchangeserver/journal/CommandJournal.java:107`). After writing a batch, `channel.force(true)` asks
the OS to flush the data (and, with `true`, file metadata such as size) to the storage device: this is the
**fsync** (**[Verified]** `src/main/java/exchangeserver/journal/CommandJournal.java:160`). Recovery opens a second
channel and `truncate`s the file to just after the last `\n` (**[Verified]**
`src/main/java/exchangeserver/journal/CommandJournal.java:263-274`).

`journal.Durable` adds two crash-safe operations (Spec D69, D71):

- `writeAtomically(target, content)`: write a temporary file, `force` it, then `Files.move(tmp, target,
  ATOMIC_MOVE, REPLACE_EXISTING)`, then fsync the directory. After a crash the target holds the old or the new
  content, never a mix. Used for snapshots, the empty journal compaction leaves, and the users file
  (**[Verified]** `src/main/java/exchangeserver/journal/Durable.java:25-36`).
- `fsyncDirectory(file)`: open the parent directory and `force` it, so a newly created or renamed file's
  directory entry survives a crash on Linux. Windows cannot open a directory as a file; that failure is ignored
  (**[Verified]** `src/main/java/exchangeserver/journal/Durable.java:50-63`).

**Minimum.** `write` returning does not mean the data is on disk; only `force` does. One `force` cost about
1.5 ms on the author's disk (**[Verified]** Spec §5), which is why batching matters. Reading uses separate
`InputStream`s (`Files.newInputStream`), so replay can read while the append channel is open. A rename is atomic
on one file system, which is what makes write-then-rename safe.

### 3.2 `CRC32C` checksums

**Here.** Every journal record and every snapshot line ends with `,<crc>`: the CRC-32C of the text before it, as 8
hex digits (**[Verified]** `src/main/java/exchangeserver/journal/Checksums.java:13-17`,
`src/main/java/exchangeserver/journal/JournalCodec.java:166-187`):

```java
// src/main/java/exchangeserver/journal/Checksums.java:13-17
public static String crcHex(String text) {
    CRC32C crc = new CRC32C();
    crc.update(text.getBytes(UTF_8));
    return String.format("%08x", crc.getValue());
}
```

**Why.** **[Verified]** Spec D69: before, a bit flip that still parsed (a `100` turned into `900`) would silently
change history. **Minimum.** `java.util.zip.CRC32C` (Java 9+) is a checksum, good at catching accidental
corruption, not a security measure.

The output digest (`core.OutputDigest`) is a different tool for a different job: an FNV-1a 64-bit hash over a
canonical text encoding of every engine output, kept running across commands so a `CHECK` record can store "the
digest of everything so far" (**[Verified]** `src/main/java/exchangeserver/core/OutputDigest.java:27-28`, `80-81`;
Spec D70).

### 3.3 `FileLock` and the `<journal>.lock` file (D65)

**Here.** To stop two processes (e.g. a running exchange and an operator's `--quarantine` or `--compact` run) from
writing the same journal, `CommandJournal.open` first acquires a `JournalLock`: an exclusive lock on a **separate,
empty file next to the journal**, `<journal>.lock` (e.g. `exchange.journal.lock`), taken before the journal is
opened at all:

```java
// src/main/java/exchangeserver/journal/JournalLock.java:37-66 (trimmed)
public static JournalLock acquire(Path journal) throws IOException {
    Path lockFile = journal.resolveSibling(journal.getFileName() + ".lock");
    try {
        Files.createFile(lockFile);     // create-new: never opens, so never closes, an existing lock file
    } catch (FileAlreadyExistsException normalAfterTheFirstRun) {
        // reuse it
    }
    Path key = lockFile.toRealPath();   // one key however the journal path is spelled
    if (!HELD.add(key)) {
        throw alreadyOpen(journal);
    }
    FileChannel channel = null;
    try {
        channel = FileChannel.open(lockFile, WRITE);
        if (channel.tryLock() == null) {
            throw alreadyOpen(journal);
        }
        return new JournalLock(key, channel);
    } catch (IOException | RuntimeException e) {
        // close the channel, leave HELD, rethrow
    }
}
```

`close()` closes the lock channel (which releases the OS lock) and removes the key from `HELD`; the empty lock file
stays on disk (**[Verified]** `src/main/java/exchangeserver/journal/JournalLock.java:70-79`; `.gitignore` ignores
`*.journal.lock`). Compaction holds a `JournalLock` itself while it replaces the journal file (Spec D71).

**Why a separate file: OS lock semantics differ.** **[Verified]** the design comment at
`src/main/java/exchangeserver/journal/JournalLock.java:13-23` and Spec D65:

| | Windows | Linux / macOS (POSIX `fcntl` locks) |
|---|---|---|
| Strength | **Mandatory**: a locked range cannot be read or written through any *other* handle, even in the same process | **Advisory**: only stops others who also ask for the lock |
| Owner | The handle (channel) that took it | The **whole process** |
| Released when | That handle closes | **Any** handle the process has on that file closes |

The journal itself is opened and closed constantly: the header check, the torn-tail cut and every `read()`
(replay, the poison-pill rebuild) use their own short-lived handles. Under POSIX semantics, the first of those
closes would silently drop a lock held on the journal. Under Windows semantics, a lock on the journal's *data*
would block those reads. A lock file that **nothing else ever opens or closes** avoids both. That is also why the
lock file is created with `Files.createFile` (create-new, which fails without opening an existing file), and why a
second acquire **in the same JVM** is refused by the in-memory `HELD` set before it touches the lock file: opening
and closing a second channel on it would, on POSIX, drop the first holder's lock.

History: the first version of D65 locked one byte at `Long.MAX_VALUE - 1` *of the journal itself*. It passed on
Windows but would have vanished on Linux/macOS at the first `read()`; it was replaced by the lock file the same day
(**[Verified]** Spec D65).

**Minimum.** `FileChannel.tryLock()` returns `null` if **another process** holds an overlapping lock, and throws
`OverlappingFileLockException` if **this JVM** already holds one on that file through another channel. A lock is
released when its channel closes, or when the process dies. **[Verified — observed]** the child-JVM test
`CommandJournalTest.Exclusive.anotherProcessIsRefusedWhileTheJournalIsOpen` passes on Windows and on Linux (Docker),
and a live `--quarantine` against a running server was refused on both ("already open").

### 3.4 Charsets

Every byte/char conversion names a charset: `UTF_8` for the journal, snapshots and the users file, `US_ASCII` for
the wire protocol (**[Verified]** e.g. `src/main/java/exchangeserver/journal/CommandJournal.java:18`,
`src/main/java/exchangeserver/gateway/ClientSession.java:26`). **Why:** **[Verified]** `CLAUDE.md`: the dev
machine's default charset is windows-1252, so relying on the default would make the code behave differently across
machines. A related trap, **[Verified — observed]**: Windows PowerShell pipes text to a program with a UTF-8
byte-order mark (`ef bb bf`) in front, which broke `--add-user` until `Main` learned to drop it
(`src/main/java/exchangeserver/Main.java:222-230`).

## 4. Networking (`java.net`, `javax.net.ssl`)

### 4.1 Sockets and timeouts

**Here.** Blocking, thread-per-connection sockets: a `ServerSocket` bound to `new InetSocketAddress(bindAddress,
port)`, where the bind address is the loopback address (127.0.0.1) unless `--bind` says otherwise, `SO_REUSEADDR`
on, `TCP_NODELAY` on each accepted socket (disables Nagle's algorithm, which would otherwise delay small writes to
batch them) (**[Verified]** `src/main/java/exchangeserver/gateway/TcpGateway.java:68-70`, `123-124`, `188`). Each
session has a reader thread doing byte-by-byte `read()` from a `BufferedInputStream`, and a sender thread writing
through a `BufferedWriter`, flushing once per burst.

**Socket read timeouts.** `socket.setSoTimeout(ms)` (the `SO_TIMEOUT` option) makes a blocking `read()` throw
`SocketTimeoutException` after that long without data. The session sets the login timeout (10 s) before login and
the idle timeout (300 s) after it; the exception ends the session with an `ERROR ... timeout` line (**[Verified]**
`src/main/java/exchangeserver/gateway/ClientSession.java:157`, `166-168`, `308`; Spec D75).

**Why.** **[Verified]** Spec §6 and Vision §5: "TCP or TLS, line protocol, thread-per-connection". **[Inferred]**
Blocking I/O with two threads per client is the simplest model to reason about; NIO or virtual threads are planned
as roadmap step 13 (Vision §7).

**Minimum.** Closing a socket's input or output stream closes the whole socket (which is why the reader never uses
try-with-resources on the input stream: **[Verified]** `src/main/java/exchangeserver/gateway/ClientSession.java:153-154`).
A blocked `read()` is woken by closing the socket from another thread, or by the read timeout.

### 4.2 TLS with JSSE

**Here.** TLS is optional, enabled by giving the server a PKCS#12 keystore (`--tls-keystore`, password in the
`ES2_TLS_PASSWORD` environment variable). `gateway.Tls` builds the JDK objects (**[Verified]**
`src/main/java/exchangeserver/gateway/Tls.java:22-50`):

- **Server:** `KeyStore.getInstance("PKCS12")` loads the key and certificate; a `KeyManagerFactory` turns it into
  key managers; `SSLContext.getInstance("TLS")` is initialised with them. The gateway then creates an
  `SSLServerSocket` from the context and limits it to TLS 1.3 and 1.2 (**[Verified]**
  `src/main/java/exchangeserver/gateway/TcpGateway.java:118-119`).
- **Client:** a `TrustManagerFactory` initialised with a truststore (or `null` for the JDK's default trusted
  certificates); the context's `SocketFactory` is passed to `ExchangeClient` (`ClientMain --tls [--truststore F]`,
  password in `ES2_TRUSTSTORE_PASSWORD`).

**Why.** **[Verified]** Spec D74: with `--bind` the gateway is reachable from the network, and without TLS secrets
and fills would cross it in clear text. Passwords come from the environment because command-line arguments are
visible to other users in the process list (`src/main/java/exchangeserver/gateway/Tls.java:13-17`).

**Minimum.** **[General knowledge]** JSSE (Java Secure Socket Extension) is the JDK's TLS implementation; the
handshake happens lazily on the first read or write of an `SSLSocket`, so with blocking sockets it runs on the
session's reader thread. The test `TcpGatewayTest.Security.tlsRoundTrip` creates a keystore with the JDK's
`keytool` and logs in over TLS.

### 4.3 Password hashing: PBKDF2 with `SecretKeyFactory`

**Here.** The users file stores, per participant, `participant:pbkdf2-sha256:<iterations>:<salt>:<hash>`.
`Credentials` hashes a secret with `SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")`, a random 16-byte salt
from `SecureRandom`, and 210,000 iterations by default; verification compares with `MessageDigest.isEqual`
(constant time) and does the same amount of work for an unknown participant (**[Verified]**
`src/main/java/exchangeserver/gateway/Credentials.java:36`, `82-87`, `120-135`):

```java
// src/main/java/exchangeserver/gateway/Credentials.java:126-135 (trimmed)
private static byte[] pbkdf2(String secret, byte[] salt, int iterations) {
    PBEKeySpec spec = new PBEKeySpec(secret.toCharArray(), salt, iterations, HASH_BITS);
    try {
        return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
    } finally {
        spec.clearPassword();
    }
}
```

**Why.** **[Verified]** Spec D74 and the class comment (`src/main/java/exchangeserver/gateway/Credentials.java:22-33`):
the secret itself is never stored; salt plus many iterations make a stolen file expensive to brute-force; constant
time and equal work keep timing from revealing which names exist.

**Minimum.** **[General knowledge]** PBKDF2 repeats an HMAC many times on purpose, so each guess costs the attacker
the same work. Tests use `Credentials.of(map, 1_000)` (few iterations) because they log in hundreds of times.

### 4.4 The token bucket (rate limiting)

**Here.** Each session holds `tokens` (up to `burst`, 2,000 by default) that refill at `ratePerSecond` (1,000)
using `System.nanoTime()`; a request takes one token or is refused with `RATE_LIMITED`; a separate check refuses
with `SESSION_BUSY` when 1,000 requests are in flight (**[Verified]**
`src/main/java/exchangeserver/gateway/ClientSession.java:279-291`; Spec D75).

**Minimum.** **[General knowledge]** A token bucket allows short bursts up to its size while capping the long-run
rate. It reads a clock, which is fine here: the gateway is outside the deterministic engine, and refused requests
are never journalled.

## 5. Time: `java.time.Clock` injection

**Here.** The `Sequencer` takes a `Clock` and is the only class in the exchange core that reads it (**[Verified]**
`src/main/java/exchangeserver/command/Sequencer.java:22`, `86-93`). Production passes `Clock.systemUTC()`
(`src/main/java/exchangeserver/core/ExchangeService.java:70`); tests pass a `MutableClock` they can move, even
backwards (**[Verified]** `src/test/java/exchangeserver/testing/MutableClock.java`). Timestamps are `Instant`s,
written to the journal with `Instant.toString()` (ISO-8601, nanosecond precision).

**Why.** **[Verified]** Spec D32/D33: the sequencer owns all stamps, and a clock that steps backwards reuses the
previous timestamp; an injected clock makes that testable.

## 6. Logging: `System.Logger`

**Here.** Three classes log, all through `System.getLogger(...)`: `Exchange`, `ExchangeService`, `TcpGateway`
(**[Verified]** `src/main/java/exchangeserver/core/Exchange.java:60`,
`src/main/java/exchangeserver/core/ExchangeService.java:94`, `src/main/java/exchangeserver/gateway/TcpGateway.java:99`).
Everything else communicates through return values and exceptions; `Main` prints with `System.out`/`System.err`.

**Minimum.** **[General knowledge]** `System.Logger` (Java 9+) is a logging facade built into the JDK. With no
logging library on the classpath it routes to `java.util.logging`, whose default console handler prints `INFO` and
above to **stderr** in a two-line format (`<date> <class> <method>` then `LEVEL: message`). You see that format in
the test output: `SEVERE: Exchange halted` followed by a stack trace (**[Verified — observed]**; `ERROR` maps to
`SEVERE`). `ExchangeServiceTest` captures log records with a `java.util.logging.Handler` to prove a throwing callback
is logged.

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

`.gitattributes` keeps journals and snapshots out of git's line-ending conversion and checks `mvnw` out with LF, so
the wrapper script runs under `sh` on Linux even when `core.autocrlf` is on (**[Verified]** `.gitattributes`).

**Minimum.** A **profile** is a named block of extra build configuration switched on with `-P<id>`. Maven
compiles **incrementally by file timestamp**: a source file restored with an *older* modification time than its
`.class` file is not recompiled. **[Verified]** this happened during the 2026-10-27 survey (a jar silently kept old
classes); `CLAUDE.md` says to use `clean` after swapping files.

## 8. Testing: JUnit 5 (Jupiter)

**Here.** Every test class uses these features (**[Verified]** across `src/test/java`):

| Feature | Meaning | Example |
|---|---|---|
| `@Test` | a test method | everywhere |
| `@Nested` | an inner (non-static) class grouping tests by behaviour; shares the outer instance's fields | `MatchingEngineTest.SelfTrade`, `.Caps`, `TcpGatewayTest.Security`… |
| `@TempDir Path dir` | JUnit creates and deletes a temporary directory per test | journals in `ExchangeTest`, `CommandJournalTest` |
| `@ParameterizedTest` + `@ValueSource` | one test body, many inputs | `OrderRequestTest.Validation`, `JournalCodecTest.Corrupt` |
| `@RepeatedTest(5)` | run the same test 5 times | `ExchangeServiceTest.Stop.racingStopNeverLosesARequest` |
| `@BeforeEach` / `@AfterEach` | setup / teardown | `TcpGatewayTest` starts a gateway on port 0 |
| `assertThrows`, `assertInstanceOf` | exceptions and types | widely |

The project convention: **one test class per production class, `@Nested` groups per behaviour; every wait in a
concurrent test has a timeout** (**[Verified]** `CLAUDE.md`, Spec §1.2). Guide 10 covers the strategy. Some tests
start a **child JVM** with `ProcessBuilder` (the cross-process lock test runs `Main --quarantine` and checks its exit code) or run the JDK's `keytool`
(TLS keystore).

## 9. JMH (benchmarks)

**Here.** Two benchmarks in `src/test/java/exchangeserver/bench`: `MatchingBenchmark` (engine cost per command at
10 / 1,000 / 100,000 price levels) and `BatchingBenchmark` (throughput and latency of fsync-per-command vs linger vs
smart batching with 1/16/64 client threads). The JMH annotation processor generates the harness code at test-compile
time (`jmh-generator-annprocess`, `pom.xml:46-51`).

**Why.** **[Verified]** Vision principle 7 "Measure before optimizing" and Spec §5: the results decided D59 (no cached
best-level pointer) and D52 (smart batching).

**Minimum.** `@State` holds per-thread or per-benchmark state, `@Setup(Level.Iteration)` rebuilds it, `@Param`
runs the benchmark once per listed value, and the return value of a `@Benchmark` method is consumed so the JIT cannot
delete the work. **[Verified]** not run on 2026-10-27; the numbers in the guides come from Spec §5 and predate the
D66–D77 changes.

## 10. PIT (mutation testing)

**Here.** `mvnw.cmd -Pmutation test` makes hundreds of small changes ("mutants") to the bytecode, e.g. `<` to
`<=`, removing a method call, returning 0, and reruns the tests against each. A mutant the tests catch is **killed**;
one they don't is a **survivor**, i.e. a behaviour no test pins down. Report: `target/pit-reports/index.html`
(**[Verified]** README, `pom.xml:127-169`). A run during the D66–D77 work generated 705 mutations and killed 640
(91%) (**[Verified — observed]**); Spec §7 holds the latest recorded numbers and the explanation of each surviving
kind.

**Why.** **[Verified]** pom comment: "the spec's 'break the code on purpose', automated" (`pom.xml:126`). The
project rule is to run it after any behavioural change and explain survivors (`CLAUDE.md`).

## 11. Docker (the Linux run)

**Here.** Not part of the build. On 2026-10-27 the whole suite was run on Linux with the official image
`eclipse-temurin:17`: the repo is mounted read-only, copied inside the container without `target/`, `mvnw` is
converted to LF, and `./mvnw -B test` runs; a second script built the jar, started the server, tried `--quarantine`
against it, and sent SIGINT and SIGTERM (**[Verified — observed]**, Spec §11; command in `CLAUDE.md`).

**Why.** **[Verified]** Spec D65 and §11: POSIX file locks and signal exit codes behave differently from Windows;
the only way to know is to run there. Result: 446 tests green, the lock refused a second process, exit 0 after both
signals.

## Key takeaways

- Java 17 with zero runtime dependencies; Maven Wrapper; JUnit 5; JMH and PIT only in opt-in profiles; Docker for
  the Linux check.
- Records with validating compact constructors make invalid values unconstructable; sealed interfaces close the set
  of commands, requests, outputs, private events and messages.
- Enum `switch` expressions are exhaustive by compiler; sealed-type dispatch uses `instanceof` chains ending in a
  `throw` because pattern `switch` is not final in Java 17.
- The concurrency toolkit: one `LinkedBlockingQueue` (+ `drainTo` = smart batching), per-session
  `ArrayBlockingQueue` with `offer`, `CompletableFuture`s (caller futures handed off via the common pool, D61;
  callbacks logged if they throw), `volatile`/`synchronized`/atomics for the few shared fields.
- Durability = `FileChannel` in `APPEND` mode + `force(true)` per batch, CRC-32C per record, write-then-rename for
  snapshots and the compacted journal, directory fsync; a `FileLock` on a separate `<journal>.lock` keeps a second
  process out on every OS.
- Security = PBKDF2 (`SecretKeyFactory`) hashed secrets with constant-time compare, JSSE TLS from a PKCS#12
  keystore, socket read timeouts, a token-bucket rate limit.
- All charsets are explicit; the clock is injected; logging is `System.Logger` → `java.util.logging` on stderr; the
  shutdown hook ends with `Runtime.halt(status)`.
- Maven compiles by timestamp: after swapping files, build with `clean`.

## Common confusions

- **"`whenComplete` runs on a pool thread."** Only `whenCompleteAsync` does. `whenComplete` runs on whichever
  thread completes the future (here: the writer), or immediately on the caller if already complete.
- **"`write` to a `FileChannel` makes data durable."** No: only `force` does. A crash between `write` and `force`
  may lose the data, or may keep part of it (hence torn-tail recovery).
- **"The CRC protects against tampering."** No: anyone who edits a record can recompute its CRC. It catches
  accidents (bit flips, truncated writes); the CHECK digest catches a different accident, a replay that behaves
  differently.
- **"The journal file is locked."** The lock is on `<journal>.lock`, an empty sibling file; the journal itself is
  never locked. The lock file staying on disk after a stop is normal.
- **"A Java `FileLock` behaves the same everywhere."** No: on Linux/macOS it is per process and is dropped when *any*
  handle on that file closes; on Windows it is per handle and mandatory.
- **"TLS is on by default."** No: it is enabled only with `--tls-keystore`. The default protection is binding to
  127.0.0.1, so nothing outside the machine can connect.
- **"`sealed` makes `instanceof` chains exhaustive."** Not in Java 17. Only enum switch expressions get that check
  here.
- **"Benchmarks run with `mvnw test`."** They are excluded from Surefire and run only with `-Pbench verify`.

## Questions to test yourself

1. Why is the exchange's request queue unbounded when the exchange enforces a capacity of 100,000?
   <details><summary>Answer</summary>So `stop()` can always add its stop marker without blocking on a full queue;
   the capacity is enforced by checking `queue.size()` under the same lock as the state check, and a full queue
   refuses with BUSY (Spec D56).</details>
2. A gateway callback and a caller's `thenAccept` are both attached to request results. On which threads do they
   run, and in what order relative to the listener?
   <details><summary>Answer</summary>The gateway callback is attached with `whenComplete` to the internal future
   before enqueueing, so it runs on the `exchange-writer` thread right after that command's listener call (D76), or
   on the caller's thread if refused at once. A caller's `thenAccept` is attached to its own handed-off future,
   completed by `whenCompleteAsync` on the common ForkJoinPool, so it runs off the writer.</details>
3. Why does `JournalLock` lock a separate `<journal>.lock` file instead of the journal itself? Give the Linux
   reason and the Windows reason.
   <details><summary>Answer</summary>Linux/macOS: POSIX locks belong to the process and are dropped when any handle
   on the file closes; the header check, the torn-tail cut and every `read()` open and close handles on the journal,
   so a lock on it would vanish almost at once. Windows: locks are mandatory, so a lock on the journal's records would
   block those same reads and the truncate. A lock file that nothing else opens or closes has neither problem.</details>
4. What does `Durable.writeAtomically` guarantee after a crash, and which steps give that guarantee?
   <details><summary>Answer</summary>The target holds either its old content or the complete new content. The new
   content goes to a temporary file that is fsynced; an atomic rename then replaces the target; the directory is
   fsynced so the rename itself survives (on Linux).</details>
5. Why does `Credentials.verify` hash the secret even when the participant does not exist?
   <details><summary>Answer</summary>So a wrong name and a wrong secret cost the same time and get the same answer;
   otherwise an attacker could time logins to learn which participant names exist.</details>
6. Without the `Runtime.halt` call in `Main`'s shutdown hook, what exit status would the server report after
   SIGTERM on Linux?
   <details><summary>Answer</summary>Normally 143 (128 + signal 15), the JVM's signal exit status. The hook's
   explicit `halt(0)` makes it 0; observed in Docker.</details>
7. You replaced `CommandJournal.java` with an older backup and ran `mvnw.cmd package`. The jar still behaves like
   the newer code. Why, and what is the fix?
   <details><summary>Answer</summary>Maven's incremental compiler compares timestamps; the restored source is older
   than its `.class` file, so it is not recompiled. Run `mvnw.cmd clean package`.</details>
