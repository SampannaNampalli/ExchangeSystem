# 12 — Change Recipes

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, plain TCP gateway | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** step-by-step recipes for the most likely changes: which files to touch, in which order, which tests to add, and which version numbers and documents must move with them | **Prerequisites:** 01, 03, 05, 06 | **Related:** 07 (all modules), 09, 10, 11
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D65); these guides in `docs/understanding/`.

## Overview

Most changes in this project touch more files than you expect, because one request crosses five layers:
the wire protocol (`gateway`), stateless validation (`domain.OrderRequest`), the stamped and journalled command (`command`, `journal`), the deterministic engine (`engine`, `book`) and the writer thread (`core`). Two version numbers guard replay:

| Number | Where | Bump it when | What happens if you forget |
|---|---|---|---|
| `Rules.VERSION` | `src/main/java/exchangeserver/domain/Rules.java:19` | any rule the engine or `OrderRequest` applies changes meaning (limits, band, cap, patterns, a new reject) | an old journal replays under new rules and silently builds a different book than clients saw **[Verified]** (comment at `Rules.java:5-14`, Spec D20, D46) |
| `JournalCodec.FORMAT_VERSION` | `src/main/java/exchangeserver/journal/JournalCodec.java:34` | the text layout of a record changes (a field added, removed, reordered, re-encoded) | old journals fail to decode ("Corrupt journal record") or, worse, decode into different values **[Inferred]** |

A journal header records both: `HEADER,<format>,<rules>` **[Verified]** (`JournalCodec.java:46-48`). `CommandJournal.open` refuses a journal whose rules version differs, and checks the header *before* changing the file **[Verified]** (`src/main/java/exchangeserver/journal/CommandJournal.java:256-269`, Spec D64). There is no migration tool: "the way across a rule change is a snapshot plus a fresh journal" (`Rules.java:12`), and snapshots are not built yet **[Verified]** (Spec §8). In practice, during development: bump the version, delete your local `*.journal` files (they are git-ignored), restart.

**Before you start any change:** the project's working rule is to *ask which collaboration mode applies* — the user writes the code and the LLM reviews and quizzes, or the LLM writes it **[Verified]** (`CLAUDE.md` "Working style"; Vision §3). If you are an AI assistant reading this, ask. If you are the user, decide.

**After any behavioural change** (the checklist every recipe ends with):
1. `mvnw.cmd test` — all green, and the compiler is warning-free (`-Xlint:all`, `pom.xml:62-65`) **[Verified]**.
2. `mvnw.cmd -Pmutation test` — PIT mutation testing; explain survivors in Spec §7 **[Verified]** (`CLAUDE.md` Tests section; `pom.xml:126-169`).
3. A new design decision gets the next D-number in `docs/ExchangeServer2_Spec.md` §3 with its real reason (next free: **D66**) **[Verified]** (`CLAUDE.md`).
4. Update the Spec's class table (§2), invariants and, if relevant, the journal format (§4) or gateway (§6) section.
5. Update the matching guide(s) in `docs/understanding/` and regenerate `FULL-CONTEXT.md` with `java docs/understanding/build-full-context.java`.
6. Don't run two Maven builds at once (they share `target/`); after swapping source files from a backup, use `mvnw.cmd clean ...` because incremental compilation goes by timestamp **[Verified]** (`CLAUDE.md` Environment; this session hit exactly that).

---

## Recipe 1 — Add an order type (e.g. a STOP order, or a "good-till-time" LIMIT)

The type set is a Java `enum` used in exhaustive `switch` expressions, so the compiler will find most of the places for you. That is deliberate **[Verified]** (`MatchingEngine.java` header comment: "a switch over OrderType with no default (the compiler rejects a missed type)", Spec D24).

```java
// src/main/java/exchangeserver/domain/OrderType.java:12-23
public enum OrderType {
    LIMIT, IOC, FOK, MARKET, POST_ONLY;

    // Only these two ever rest on the book.
    public boolean canRest() {
        return this == LIMIT || this == POST_ONLY;
    }
}
```

Steps, in order:

1. **Decide the semantics first, with a numeric scenario** (Vision §3 rule 1). For each type you must answer: does it have a price? does it rest? what happens when it fully fills / partly fills / nothing crosses? which state-dependent checks apply (band, cap, duplicate id)? Write the row for the Spec §2 order-type table before coding.
   - A **STOP** order is a big change: it is *not* in the book when placed; it waits for a trade price to reach its trigger, then becomes a market/limit order. That needs a new structure (a trigger list in the engine, ordered deterministically), and triggered orders generate trades inside *another* command's processing. **[Inferred]** Expect a new D-number for "where triggered orders live and in what order they fire".
   - A **GTT** ("good till time") LIMIT needs expiry. The engine must not read a clock (determinism, `MatchingEngine.java:25-26`), so expiry must arrive as a *command* stamped by the `Sequencer` (e.g. an `EXPIRE` command), not as a timer inside the engine **[Inferred from the determinism rule]**.
2. **`domain/OrderType.java`** — add the constant; update `canRest()` if it rests.
3. **`domain/OrderRequest.java`** — add a typed factory (`limit`, `ioc`, … at `OrderRequest.java:76-94`) and make sure `of(...)` (`:61-74`) handles price presence correctly: today only `MARKET` has no price. If your type has an extra field (a trigger price), see Recipe 4.
4. **`engine/MatchingEngine.java`** — compile; the `switch (order.getOrderType())` at `MatchingEngine.java:155-170` will fail to compile until you add a case. Also review:
   - the pre-checks block (`:110-133`): band check applies to any `hasPrice()` order; cap applies to `canRest()` types;
   - `matchLimit` (`:183-193`) if the type's worst acceptable price is not simply its price;
   - FOK-style "all or nothing" needs a pre-check like `canBeFilled` (`:202-214`), because the matching loop mutates as it goes.
5. **`domain/Order.java`** — `hasPrice()` is `orderType != MARKET` (`Order.java:67-69`); adjust if your type has no price.
6. **`journal/JournalCodec.java`** — encoding uses `getOrderType().name()` and decoding `OrderType.valueOf` (`:80`, `:107`), so a new constant needs no codec change **unless** the price/extra-field layout differs. If it does, bump `FORMAT_VERSION` (Recipe 4).
7. **`gateway/Protocol.java`** — `parseNew` (`Protocol.java:88-100`) treats `MARKET` as "4 fields, no price", every other type as "5 fields with price". Add a branch if your type's syntax differs, and update the protocol comment block at `Protocol.java:20-38`.
8. **`domain/Rules.java`** — bump `VERSION` (`:19`). A journal written before the change cannot contain the new type, but an old build reading a new journal would fail; more importantly any rule-like behaviour change should bump it **[Inferred]**.
9. **Tests** (one class per production class, `@Nested` per behaviour):
   - `MatchingEngineTest` — a new `@Nested class YourType` with the three outcome cases (fully fills / partly / nothing crosses), plus band/cap/duplicate interactions. The helper style is in `MatchingEngineTest.java:39-79`.
   - `OrderRequestTest` — the factory and `of(...)` validation.
   - `JournalCodecTest.RoundTrip` — the new type round-trips.
   - `ProtocolTest.Parse` — the new syntax and its malformed variants.
   - `OrderBookTest.InvariantsUnderRandomTraffic` (`OrderBookTest.java:258`) already draws from `OrderType.values()`, so the new type enters the 30,000-command invariant check automatically **[Verified]** (`OrderBookTest.java:262-283` uses `types = OrderType.values()`). Make sure its price/qty generation still makes sense.
   - `TcpGatewayTest.Trading` — one end-to-end case.
10. **Docs:** Spec §2 order-type table, new D-number, README session example if user-visible; guide 07 (modules/engine) and guide 06.

---

## Recipe 2 — Add a reject reason / a new risk check

Example: "reject an order whose quantity exceeds 5% of the visible opposite-side volume".

1. **Stateless or stateful?** If the check needs only the request's own fields (e.g. "quantity must be a multiple of 10"), it belongs in `OrderRequest`'s constructor (`OrderRequest.java:24-58`) and is answered with `ERROR` at the gateway; it never reaches the queue or the journal **[Verified]** (Spec D47, `ClientSession.java:245-...` catches `IllegalArgumentException` from `OrderRequest.of`). If it needs book or history state, it is a **state-dependent reject**: it runs in the engine *after* journalling, and replay reproduces it **[Verified]** (`RejectReason.java:3-8`, Spec D13).
2. **`engine/RejectReason.java`** — add the constant with a one-line comment.
3. **`engine/MatchingEngine.java`** — add the check in the "checks that change nothing" block (`:110-133`). **Order matters**:
   - The duplicate-client-order-id check comes first and *consumes* the id even when a later check rejects (`putIfAbsent` at `:110-114`) **[Verified]**. Every new order uses up its client order id (Spec D49). Keep your check after it.
   - The check must not mutate state before returning, otherwise a rejected order would leave traces that replay reproduces but no client saw explained **[Inferred]**. The only allowed mutation is the client-order-id map.
   - Return via `rejected(order.getOrderId(), order, RejectReason.YOUR_REASON)` (`:175-178`). `Response`'s constructor enforces "REJECTED ⇔ reason non-null" and "REJECTED ⇒ filled = 0" (`Response.java:33-51`).
4. **`domain/Rules.java`** — put any constant here (e.g. `MAX_PERCENT_OF_BOOK`) and **bump `VERSION`**: an old journal replayed with the new check would reject orders that originally traded **[Verified]** (`Rules.java:5-12`).
5. **Gateway** — nothing to change: `Protocol.order` prints `reason=<enum name>` (`Protocol.java:148-152`).
6. **Tests:** `MatchingEngineTest.Risk` (boundary values: exactly at the limit, one above); `ExchangeTest.LivePath.stateDependentRejectsAreJournalled`-style test proving it is journalled and replays to the same outcome; `ResponseTest` needs nothing new.
7. **Docs:** Spec D-number, §2 status/reject list ("Any type can be REJECTED for…"), guide 07 (modules/engine).

---

## Recipe 3 — Change a business-rule constant (band %, open-order cap, max quantity)

```java
// src/main/java/exchangeserver/domain/Rules.java:19-33 (trimmed)
public static final int VERSION = 1;
public static final long MAX_ORDER_QUANTITY = 1_000_000_000L;
public static final long MAX_PRICE = Long.MAX_VALUE / MAX_ORDER_QUANTITY;
public static final Pattern PARTICIPANT = Pattern.compile("[A-Za-z0-9_]{1,16}");
public static final Pattern CLIENT_ORDER_ID = Pattern.compile("[A-Za-z0-9_.-]{1,32}");
public static final int PRICE_BAND_PERCENT = 10;
public static final int MAX_OPEN_ORDERS_PER_PARTICIPANT = 10_000;
```

1. Change the constant in `Rules.java`.
2. **Bump `VERSION`** in the same edit **[Verified]** (the file's own header comment).
3. Check derived arithmetic:
   - `MAX_PRICE` is derived so that `price × quantity` never overflows a `long` (`Rules.java:23-24`); raising `MAX_ORDER_QUANTITY` lowers `MAX_PRICE` **[Verified]**.
   - `withinBand` multiplies prices by `100 ± PRICE_BAND_PERCENT`; the comment's no-overflow argument assumes "times at most 110" (`Rules.java:38-43`). A band above ~99% or a `MAX_PRICE` change needs that argument redone **[Inferred]**.
   - `matchLimit` computes the market collar with the same percentage, using floor for buys and ceil for sells so it equals `withinBand` exactly (`MatchingEngine.java:183-193`) **[Verified]**. If you change the band *formula* (not just the number), change both places together — `RulesTest.bandIsInclusiveAtBothEdges` and `MatchingEngineTest.Market.sellCollarRoundsInward` guard the agreement.
   - Patterns must never allow `,` or whitespace: they are the journal and wire separators (`Rules.java:25`) **[Verified]**.
4. Update boundary tests: `RulesTest`, `MatchingEngineTest.Risk` (`bandIsInclusiveAroundTheLastTrade`, `openOrderCapRejectsRestingTypesButNotImmediateOnes`), `OrderRequestTest.Validation`.
5. Locally: delete old `*.journal` files (startup now refuses them with "written under rules version N").
6. Docs: Spec §2 `domain.Rules` row; glossary values in guide 13.

---

## Recipe 4 — Add a field to orders (e.g. a free-text `account` tag)

This touches every layer and the journal format.

1. **`domain/Rules.java`** — define validation (pattern, length) for the field; no `,` or whitespace. Bump `VERSION` if the field affects matching; a pure tag arguably does not, but the journal format does change (next step) **[Inferred]**.
2. **`domain/OrderRequest.java`** — add the field, validate it in the private constructor (the only field-validation layer, Spec D47), add it to every factory, to `of(...)`, `equals`, `hashCode`, `toString` (`OrderRequest.java:125-146`). `OrderRequestTest.equalityCoversEveryField` will remind you.
3. **`domain/Order.java`** — copy it in the constructor (`Order.java:30-45`) and add a getter.
4. **`journal/JournalCodec.java`** — add the field to `encode` (`:71-82`) and `decode` (`:105-111`), change `requireFieldCount(f, 10)` to 11, update the format comment (`:18-26`), and **bump `FORMAT_VERSION`** (`:34`). Decoding must keep going through `OrderRequest.of(...)` so the journal applies the same stateless rules as the gateway **[Verified]** (`JournalCodec.java:28-29`).
5. **Engine outputs** — if the field must reach clients, add it to `book.Fill` (`Fill.java:11-12`), `engine.Execution` (`Execution.java:11-12`), possibly `engine.Response`, and fill it in `OrderBook.fillBest` (`OrderBook.java:92-93`) and `MatchingEngine.process` (`:146-150`).
6. **Gateway** — accept it in `Protocol.parseNew` (`Protocol.java:88-100`) and `ClientMessage.NewOrder` (`ClientMessage.java:14-16`); pass it in `ClientSession.newOrder` (`ClientSession.java:245`); print it in `Protocol.fill` / `order` (key=value lines are designed so fields can be added without breaking parsers, `Protocol.java:27`).
7. **Line length:** a NEW line must stay ≤ 256 bytes (`Protocol.MAX_LINE_LENGTH`, `Protocol.java:44`) — check the worst case.
8. **Tests:** `OrderRequestTest`, `OrderTest`, `JournalCodecTest.RoundTrip` and `Corrupt` (wrong field count), `ProtocolTest`, one `TcpGatewayTest` case, `ExchangeTest.Restart` (survives replay).
9. **Docs:** Spec §4 journal format (format version 3), §2, D-number; guide 06.

---

## Recipe 5 — Add a protocol message (e.g. `STATUS <orderId>`)

1. **Decide where it is answered.** Session-local messages (`PING`, `QUIT`, `LOGIN`) never touch the exchange. Anything that reads book state must go **through the queue** — no other thread may read the book (`CLAUDE.md` "Single writer", Spec D57). See Recipe 6 for the query itself.
2. **`gateway/ClientMessage.java`** — add a record to the sealed interface (`ClientMessage.java:9-32`).
3. **`gateway/Protocol.java`** — add a `case` to `parse` (`Protocol.java:53-86`) using `arity` (`:102-106`) and `number` for numeric fields; add a formatter (`static String yourLine(...)`) producing `TYPE key=value ...`; update the comment block (`:20-38`).
4. **`gateway/ClientSession.java`** — handle it in `handle` (`ClientSession.java:182-...`). Messages after the `LOGIN first` guard (`:203-205`) require a login. If it calls the exchange, use the **callback** variant of `ExchangeService` (`ExchangeService.java:169-179`) so the reply is sent from the writer thread in order; the callback must not block (`ExchangeService.java:166-168`). Use `failureLine(...)` (`:316`) for `ExchangeException` refusals.
5. **Tests:** `ProtocolTest.Parse` (well-formed and malformed — the parameterized `malformedLinesAreProtocolErrors` list), `ProtocolTest.Format`, a `TcpGatewayTest` end-to-end case using `ExchangeClient.awaitPrefix`.
6. **Docs:** `Protocol.java` comment (it is *the* protocol documentation per `CLAUDE.md`), README if user-facing, guide 07 (modules/gateway), guide 06 wire tables.

---

## Recipe 6 — Add a read-only query through the queue (e.g. "my open orders")

Reads are requests like any other, so they see the state after every earlier request and before every later one, and they are never journalled **[Verified]** (`Request.java:24-26`, `Exchange.java:145-148`).

1. **Engine/book** — add a read method (e.g. `OrderBook.ordersOf(participant)` returning immutable copies, never `Order` or `OrderNode` references; the engine and gateway must not hold book internals, Spec D44). Expose it through `MatchingEngine`.
2. **`core/Request.java`** — add `record OpenOrders(String participant) implements Request<List<...>>` to the sealed interface (`Request.java:16-34`). Validate arguments in its compact constructor, on the caller's thread, like `Snapshot` does (`:26-32`): "a bad read must not reach (and halt) the writer thread".
3. **`core/Exchange.java`** — in `process` (`Exchange.java:118-166`):
   - the stamping loop (`:120-135`) only stamps `Submit` and `Cancel`; a read leaves `p.command == null` and is not journalled — good;
   - in the processing loop add a branch next to the `Snapshot` one (`:145-148`) that completes the future with your result and `continue`s (no listener call).
   - Add a convenience method beside `snapshot` (`:201-203`) if tests or tools need it.
4. **`core/ExchangeService.java`** — add the future variant (wrap with `handOff`, like `snapshot` at `:150-152`, so callers never get the writer's own future — D61) and, if the gateway needs it, a callback variant (`:177-179`).
5. **Gateway** — Recipe 5.
6. **Tests:** `ExchangeTest` (not journalled; `interceptorSeesEveryCommandLiveInOrder` style), `ExchangeServiceTest.HandIn.readsGoThroughTheQueueInOrder` style, engine unit tests.
7. No `Rules.VERSION` bump: reads do not change replay **[Inferred]**.

---

## Recipe 7 — Change batching or queue settings, and add a CLI flag

Settings live in a record with validated fields and `with…` copiers:

```java
// src/main/java/exchangeserver/core/ExchangeService.java:59-70 (trimmed)
public record Settings(int queueCapacity, int maxBatchSize, long lingerNanos, Clock clock) {
    public Settings {
        Objects.requireNonNull(clock, "clock");
        if (queueCapacity <= 0 || maxBatchSize <= 0 || lingerNanos < 0) {
            throw new IllegalArgumentException("Bad settings");
        }
    }
    public static Settings defaults() {
        return new Settings(100_000, 1_000, 0, Clock.systemUTC());
    }
```

`Main` has **no** flags for these; it calls `ExchangeService.start(journal, gateway)`, which uses `Settings.defaults()` **[Verified]** (`Main.java:63`, `ExchangeService.java:123-125`). To make one configurable:

1. **`Main.java`** — add a `case "--queue-capacity" -> …` to the argument `switch` (`Main.java:34-39`); parse errors are already turned into the usage message and exit 1 (`:41-45`). Update the usage string and the class comment (`Main.java:12-21`).
2. Build `Settings.defaults().withQueueCapacity(n)` and call the three-argument `ExchangeService.start(journal, gateway, settings)` (`ExchangeService.java:119`).
3. For gateway limits (sessions 256, outbound queue 10,000 lines), use the three-argument `TcpGateway(port, maxSessions, outboundCapacity)` constructor **[Verified]** (`TcpGateway.java:58-68`).
4. Update README "Run" section and `CLAUDE.md` Commands.
5. **Changing the defaults**: `maxBatchSize` 1,000 and smart batching (linger 0) were chosen by measurement (Spec D52, §5). Re-run `BatchingBenchmark` (`mvnw.cmd -Pbench verify -Djmh.args="BatchingBenchmark -f 1 -wi 2 -w 2s -i 3 -r 3s -t 16"`) and record the numbers in Spec §5 before claiming an improvement (Vision principle 7, "measure before optimizing").
6. Tests: `ExchangeServiceTest.settingsAreValidated`; read writer stats only **after `stop()`** — they lag the replies (`ExchangeService.java:101-106`, Spec §10 #1).

---

## Recipe 8 — Add a dependency

The production jar has **zero** runtime dependencies; JUnit, JMH and PIT are test-scope or plugin-only **[Verified]** (`pom.xml:33-52`, `:126-169`).

1. Add it to `pom.xml` `<dependencies>`. Prefer a version property next to `junit.version` (`pom.xml:17-18`). Use `<scope>test</scope>` unless production code needs it.
2. A production dependency changes deployment: `java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar` works today only because there is nothing else on the classpath; the jar plugin (`pom.xml:78-89`) sets `Main-Class` but no `Class-Path` and there is no shading. You would need the shade/assembly plugin or a `-cp` with the dependency jar **[Inferred]**.
3. Keep the build warning-free under `-Xlint:all` (`pom.xml:62-65`). Annotation processors produce a `processing` warning, which is why `-Xlint:-processing` is set for JMH's processor (`pom.xml:61-64`).
4. If the dependency brings test classes that must not run under surefire, extend the `<excludes>` (`pom.xml:72-76`); JMH benchmarks are excluded there.
5. Update guide 02 (tech stack) and Spec §8 "Decisions confirmed" style note in the Vision if it is architectural (e.g. the Spring Boot drop is recorded in Vision §8).
6. Determinism check: a library used inside the engine must not read clocks, randomise or iterate hash maps in an order-dependent way (`MatchingEngine.java:25-26`).

---

## Recipe 9 — Add a new test

Conventions **[Verified]** (`CLAUDE.md` Tests; Spec §1):

- **One test class per production class**, same package under `src/test/java/exchangeserver/`, with `@Nested` inner classes per behaviour (`class Limit`, `class Risk`, …).
- **Every wait has a timeout**; never synchronise with `Thread.sleep` (`ExchangeServiceTest.java:39-41`, `WAIT = Duration.ofSeconds(30)`). Use `future.get(WAIT…)`, `CountDownLatch.await(timeout)`, `ExchangeClient.awaitPrefix(prefix, WAIT)`.
- **Never read engine/book state from the test thread while the writer runs.** Go through a future (`service.snapshot(depth)`), which is a happens-before edge (`CLAUDE.md` Single writer).
- **Writer stats** (`batches()`, `processed()`, `largestBatch()`) only after `assertTrue(service.stop(WAIT))` (`ExchangeServiceTest.java:39-41`; this session's flaky-test fix).
- **Seams** (package-private, so the test must be in the same package):
  - `core.CommandInterceptor` — inject a poison pill (throw) or block the writer (the `Gate` class in `ExchangeServiceTest.java`, released by a latch). `ExchangeService.start(path, listener, settings, interceptor)` and `Exchange.open(path, clock, interceptor)` accept it.
  - `OrderBook.checkInvariants()` and `ordersAt(side, price)` (`OrderBook.java:178-234`).
  - `MatchingEngine.book()` (`MatchingEngine.java:102-104`) and `Exchange.engine()` — owning thread only.
  - `testing.MutableClock` — a settable, thread-safe `Clock` (`src/test/java/exchangeserver/testing/MutableClock.java`).
- **Files:** use JUnit's `@TempDir Path dir` for journals; to prove a handle was closed use `CommandJournal.openJournals()`, not file deletion (Windows delete sharing, Spec §1.5).
- **Regression tests for a fix:** put the old code back and confirm the new test fails, as Spec §9 and §10 did (`git show HEAD:path > path`, run, restore — then `mvnw.cmd clean test`, because restored files keep old timestamps).
- Then run `-Pmutation` and look at survivors in your area (`target/pit-reports/index.html`).

Skeleton:

```java
// sketch, in src/test/java/exchangeserver/engine/MatchingEngineTest.java
@Nested
class YourBehaviour {

    @Test
    void describesTheRuleInASentence() {
        limit("s", SELL, 100, 5);                       // helpers stamp seq/id like the sequencer
        Response r = limit("b", BUY, 100, 2);
        assertEquals(OrderStatus.FILLED, r.status());
        book.checkInvariants();
    }
}
```

---

## Recipe 10 — Change journal behaviour (durability, recovery)

1. Read guide 07 (modules/journal) first: `open` is **lock the separate `<journal>.lock` file → open the journal channel → check header → cut torn tail → write header if empty** (`CommandJournal.java:71-103`), and that order is a decision (Spec D64, D65). Never take a lock on the journal file itself: on Linux/macOS, closing *any* handle on a file drops the process's POSIX locks on it, and `read()`, the header check and the torn-tail cut all open and close handles on the journal (`CommandJournal.java:198-249`). If you touch the lock, run the child-JVM test on Linux too: `docker run --rm -v "$PWD":/w -w /w eclipse-temurin:17 ./mvnw -B test -Dtest=CommandJournalTest`.
2. Never weaken "nothing is processed before its batch is fsynced" (`Exchange.java:137`, `CommandJournal.java:127-131`): that is the write-ahead rule, and the `ExchangeTest.LivePath.visibleImpliesDurable` test checks it.
3. Removing a `force(true)` is a *survivor* PIT cannot catch (Spec §7) — no unit test can observe an fsync. Treat such changes as review-only.
4. Format changes: Recipe 4 step 4 (`FORMAT_VERSION`).

---

## Key takeaways

- Two version numbers guard replay: `Rules.VERSION` (meaning) and `JournalCodec.FORMAT_VERSION` (layout). Bump the right one in the same edit as the change.
- Stateless validation lives only in `OrderRequest`; stateful rejects live in the engine's "checks that change nothing" block, after the duplicate-id check, and are journalled.
- The exhaustive `switch` over `OrderType` and sealed interfaces (`Command`, `Request`, `ClientMessage`, `EngineOutput`) let the compiler list the places a new variant must be handled — but `instanceof` chains (e.g. `Exchange.apply`, `Replay.feed`, `ClientSession.handle`) are *not* checked by the compiler.
- Reads go through the writer queue as non-journalled requests; nobody else touches the book.
- Every change ends with: tests, `-Pmutation`, a D-number with its real reason, Spec tables, these guides, and `FULL-CONTEXT.md` regeneration.
- `Main` has only `--port`, `--journal`, `--quarantine`; every other setting is a code default.
- Ask which collaboration mode applies before writing production code.

## Common confusions

- **"A new `RejectReason` is just an enum constant."** It is a rule change: old journals would replay differently. Bump `Rules.VERSION`.
- **"Adding an `OrderType` constant needs no journal change."** True for the codec (it writes `name()`), but only if the record layout is unchanged; and an older build cannot read a journal containing the new name.
- **"I'll validate the new field in the engine too, to be safe."** Spec D47 removed exactly that duplication; `Order` can only be built from a valid `OrderRequest`.
- **"Reads can just call `engine.snapshot()` from the gateway thread."** That is a data race; reads go through the queue (D57).
- **"`instanceof` chains are as safe as switches."** Java 17 has no pattern-matching `switch` (it was a preview), so `Exchange.apply` and `Replay.feed` use `instanceof` and would silently ignore a new `Command` type. Grep for them.

## Questions to test yourself

1. You change `PRICE_BAND_PERCENT` from 10 to 15 and restart on yesterday's journal. What happens, and why is that the desired behaviour?
<details><summary>Answer</summary>If you also bumped <code>Rules.VERSION</code> (as you must), <code>CommandJournal.open</code> refuses the journal ("written under rules version 1; this build uses 2") without modifying it (D46, D64). That is desired because replaying yesterday's orders under a 15% band could accept orders that were rejected live, building a book clients never saw. If you forgot the bump, startup succeeds and the replayed book may silently differ.</details>

2. Where exactly does a new state-dependent check go, and what must it not do before returning?
<details><summary>Answer</summary>In <code>MatchingEngine.process</code>'s "checks that change nothing" block (<code>MatchingEngine.java:110-133</code>), after the duplicate-client-order-id check. It must not mutate the book or engine state before returning a reject; the only mutation allowed before a reject is the client-order-id map entry, which every order consumes (D49).</details>

3. You add a `Command` subtype `ExpireCommand`. Name three places the compiler will *not* flag.
<details><summary>Answer</summary><code>Exchange.apply</code> (throws "Not an engine command" at runtime), <code>Exchange.process</code>'s stamping loop (only stamps Submit/Cancel), <code>Replay.feed</code> (ignores unknown types silently), <code>Replay.Verifier</code>, <code>JournalCodec.encode</code> (throws "Unknown command type") and <code>decode</code> (unknown record word). The sealed <code>permits</code> clause itself <em>will</em> fail to compile until you list the new record.</details>

4. Why does a protocol message that reads the book use the callback variant of `ExchangeService`, not the future variant?
<details><summary>Answer</summary>The callback is attached before the request is queued and runs on the writer thread right when the result is ready, so the reply line is offered to the session's outbound queue in the same order as the command's market data (reply before market data, D53). The future variant completes on the common pool (D61), which would lose that ordering.</details>

5. Which test convention would a new `ExchangeServiceTest` violate if it asserted `service.batches()` right after awaiting all its futures?
<details><summary>Answer</summary>The stats are updated after the batch's futures complete (<code>ExchangeService.java:259-263</code>), so they can lag; read them only after <code>stop()</code>, whose <code>join</code> gives a happens-before edge. This exact race made <code>waitingRequestsShareOneBatch</code> flaky (Spec §10 #1).</details>
