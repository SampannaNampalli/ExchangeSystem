# 12 — Change Recipes

> **Project:** ExchangeServer2 | Single-instrument limit order book: price-time matching engine, write-ahead journal with replay, single writer thread, TCP/TLS gateway with authenticated participants | Stack: Java 17, Maven wrapper, JUnit 5 (+ JMH, PIT)
> **This guide:** step-by-step recipes for the most likely changes: which files to touch, in which order, which tests to add, and which version numbers, digests and documents must move with them | **Prerequisites:** 01, 03, 05, 06 | **Related:** 07 (all modules), 09, 10, 11
> **Repo layout summary:** `pom.xml` + Maven wrapper (`mvnw`, `mvnw.cmd`) at the root; production code in `src/main/java/exchangeserver/` (packages `domain`, `book`, `engine`, `command`, `journal`, `core`, `gateway`, `client`, plus `Main`); tests mirror it in `src/test/java/exchangeserver/` (plus `bench/` JMH benchmarks and `testing/MutableClock`); design docs in `docs/` (`ExchangeServer2_Vision.md` = purpose/roadmap, `ExchangeServer2_Spec.md` = decisions D1–D77); these guides in `docs/understanding/`.

## Overview

Most changes touch more files than you expect, because one request crosses six layers: the wire protocol and session limits (`gateway`), stateless validation (`domain.OrderRequest`), the stamped and journalled command (`command`, `journal`), the deterministic engine (`engine`, `book`), the ledger that numbers private events and digests outputs (`core.Ledger`), and the writer thread (`core.Exchange`, `core.ExchangeService`).

Three things guard replay. Know which one your change moves:

| Guard | Where | Moves when | If you forget |
|---|---|---|---|
| `Rules.VERSION` (now 2) | `src/main/java/exchangeserver/domain/Rules.java:21` | a rule the engine or `OrderRequest` applies changes meaning (limits, band, caps, patterns, a new reject) | an old journal replays under new rules **[Verified]** (comment `Rules.java:5-16`, Spec D20, D46) |
| `JournalCodec.FORMAT_VERSION` (now 3) | `src/main/java/exchangeserver/journal/JournalCodec.java:41` | the text layout of a record changes | old journals fail to decode or decode into different values **[Inferred]** |
| The output digest in `CHECK` records | `src/main/java/exchangeserver/core/OutputDigest.java:48`, written by `Exchange.process` at `src/main/java/exchangeserver/core/Exchange.java:200-203` | *anything* that changes what the engine outputs for the same commands | nothing to forget: replay of an old journal stops with `ReplayException(DIVERGED)` at the next CHECK **[Verified]** (`src/main/java/exchangeserver/core/Replay.java:80-88`, Spec D70) |

The header records format, rules version and the base sequence: `HEADER,3,<rules>,<base>,<crc>` **[Verified]** (`JournalCodec.java:58-60`). `CommandJournal.open` refuses a journal with records written under other rules, but lets a journal **with no records** adopt the new version **[Verified]** (`src/main/java/exchangeserver/journal/CommandJournal.java:241-261`, Spec D71). That gives the one supported way across a behaviour or rules change:

1. Stop the exchange (old build).
2. `java -jar old.jar --journal exchange.journal --compact` — writes `exchange.journal.snapshot-<N>`, archives the journal, leaves an empty journal with base N.
3. Start the new build on the same journal: it adopts the new rules version and loads the snapshot (a snapshot is state, not commands, so it does not depend on rules).

During development you can also just delete your local `*.journal*` files (they are git-ignored).

**Before you start any change:** ask which collaboration mode applies — the user writes and the LLM reviews, or the LLM writes **[Verified]** (`CLAUDE.md` "Working style"; Vision §3).

**After any behavioural change** (every recipe ends with this):
1. `mvnw.cmd test` — all green, warning-free under `-Xlint:all` **[Verified]** (`pom.xml:62-65`).
2. `mvnw.cmd -Pmutation test` — explain survivors in Spec §7.
3. Next free decision number in Spec §3: **D78** **[Verified]** (Spec §3 ends at D77).
4. Spec §2 class table, invariants, §4 journal format or §6 gateway as relevant.
5. These guides, then `java docs/understanding/build-full-context.java`.
6. Optionally the Linux run (Recipe 13).
7. One Maven build at a time; after restoring files from a backup use `mvnw.cmd clean …` (timestamps) **[Verified]** (`CLAUDE.md` Environment).

---

## Recipe 1 — Add an order type (e.g. STOP, or a good-till-time LIMIT)

`OrderType` is an enum used in an exhaustive `switch`, so the compiler lists most places for you **[Verified]** (`src/main/java/exchangeserver/engine/MatchingEngine.java:242-259`).

```java
// src/main/java/exchangeserver/domain/OrderType.java:12-23 (trimmed)
public enum OrderType {
    LIMIT,
    IOC,
    FOK,
    MARKET,
    POST_ONLY;

    public boolean canRest() {
        return this == LIMIT || this == POST_ONLY;
    }
}
```

1. **Decide the semantics with a numeric scenario first** (Vision §3): price or not, rests or not, outcome for full/partial/no fill, which checks apply (band, caps, duplicate id) and how **self-trade prevention** applies — the match loop stops at the participant's own resting order for every type (`MatchingEngine.java:224-227`, D67).
   - **STOP** waits for a trigger and fires inside another command's processing: a new deterministic structure and a new D-number **[Inferred]**.
   - **GTT** needs expiry as a *command* stamped by the `Sequencer` (like `CANCEL_ALL`), never a timer in the engine (determinism) **[Inferred]**.
2. `domain/OrderType.java` — constant and `canRest()`.
3. `domain/OrderRequest.java` — factory and `of(...)` price handling (only MARKET has no price today).
4. `engine/MatchingEngine.java` — the `switch` at `:242-259` won't compile until you add a case. Review the check block `:182-213` (band for priced orders; caps only if `canRest()`; FOK-style pre-checks via `fillability`, `:299-315`, which stops at the own order), `matchLimit` (`:274-291`) and the STP stop (`:224-227`).
5. `engine/Response.java` — if the type can end in a new status/reason combination, extend the invariants (`Response.java:18-56`).
6. `core/OutputDigest.java` — the digest encodes the response field by field (`:48-84`); a new *field* in `Response` must be added there, or two different outputs would digest alike.
7. `gateway/Protocol.java` — `parseNew` (`:102-114`) treats MARKET as "no price"; update the comment block (`:16-45`).
8. `domain/Rules.java` — bump `VERSION` (`:21`) and add the reason to the comment above it.
9. Tests: a `@Nested` group in `MatchingEngineTest` (three outcomes, band/cap/STP interactions), `OrderRequestTest`, `JournalCodecTest.RoundTrip`, `ProtocolTest.Parse`, a `TcpGatewayTest.Trading` case. `OrderBookTest.InvariantsUnderRandomTraffic` draws from `OrderType.values()`, so the type enters the random invariant check automatically **[Verified]** (`src/test/java/exchangeserver/book/OrderBookTest.java`).
10. Docs: Spec §2 order-type table and a D-number.

---

## Recipe 2 — Add a reason or a risk check

Reasons are one enum since D67:

```java
// src/main/java/exchangeserver/engine/Reason.java:12-28 (trimmed)
public enum Reason {
    DUPLICATE_CLIENT_ORDER_ID, PRICE_OUTSIDE_BAND, MAX_OPEN_ORDERS, BOOK_FULL, POST_ONLY_WOULD_CROSS,
    SELF_TRADE_PREVENTED;
    public boolean isReject() {
        return this != SELF_TRADE_PREVENTED;
    }
}
```

`Response` enforces the pairing: a reject reason only with REJECTED, `SELF_TRADE_PREVENTED` only with CANCELLED **[Verified]** (`src/main/java/exchangeserver/engine/Response.java:33-38`).

1. **Stateless or stateful?** Request-only checks belong in `OrderRequest` and are answered `ERROR` at the gateway, never journalled (D47). Book- or history-dependent checks run in the engine after journalling and are replayed (D13).
2. `engine/Reason.java` — add the constant. If it is **not** a reject (it can follow trades, like STP), make `isReject()` return false for it and decide which status it goes with in `Response`.
3. `engine/MatchingEngine.java` — the check order in `process` is **[Verified]** (`:182-213`):
   1. duplicate client order id (consumes the id even if a later check rejects, D49);
   2. price band (priced orders, after the first trade);
   3. caps — only for orders that would rest: `MAX_OPEN_ORDERS` or `BOOK_FULL` unless the order fully trades now (D68);
   4. FOK fillability (stops at the own order: `SELF_TRADE_PREVENTED` if that is what blocked it);
   5. POST_ONLY cross.
   Put your check where its meaning fits; it must not mutate anything before rejecting (only the id map). Use `rejected(...)` (`:265-268`).
4. `domain/Rules.java` — constant here, **bump `VERSION`**.
5. Gateway: nothing — `Protocol` prints `reason=<name>` (`src/main/java/exchangeserver/gateway/Protocol.java:174-178`).
6. The digest already includes `r.reason()` (`OutputDigest.java:53-55`) **[Verified]**.
7. Tests: boundaries in `MatchingEngineTest` (the `Caps` group shows the package-private `Limits` seam for caps too large to fill in a test, `MatchingEngine.java:50-54`); `ResponseTest` for any new pairing; an `ExchangeTest` case proving it is journalled and replays.

---

## Recipe 3 — Change a business-rule constant

```java
// src/main/java/exchangeserver/domain/Rules.java:21-37 (trimmed)
public static final int VERSION = 2;
public static final long MAX_ORDER_QUANTITY = 1_000_000_000L;
public static final long MAX_PRICE = Long.MAX_VALUE / MAX_ORDER_QUANTITY;
public static final int PRICE_BAND_PERCENT = 10;
public static final int MAX_OPEN_ORDERS_PER_PARTICIPANT = 10_000;
public static final int MAX_RESTING_ORDERS = 1_000_000;
```

1. Change the constant and bump `VERSION` in the same edit.
2. Check derived arithmetic: `MAX_PRICE` keeps `price × quantity` inside a `long`; `withinBand` (`Rules.java:44-47`) and `matchLimit` (`MatchingEngine.java:274-291`) must agree exactly (floor for buys, ceil for sells) — `RulesTest` and the `MatchingEngineTest.Market` collar tests guard it. Since D66 the collar's reference before the first trade is the opposite touch; a band change affects that too.
3. Existing journals: compact with the old build, start the new one (Overview).
4. Tests and docs: `RulesTest`, `MatchingEngineTest.Risk`/`Caps`, Spec §2 `Rules` row, guide 13.

---

## Recipe 4 — Add a journal record type or a field

Format v3 seals every record with a CRC-32C **[Verified]**:

```java
// src/main/java/exchangeserver/journal/JournalCodec.java:166-168
static String seal(String text) {
    return text + SEPARATOR + Checksums.crcHex(text) + END_OF_RECORD;
}
```

**A new record type** (e.g. `EXPIRE`):
1. `command/` — a record implementing `Command` with validation in its compact constructor, and add it to `permits` (`src/main/java/exchangeserver/command/Command.java`); a `stampExpire` in `Sequencer` (pattern: `stampCancelAll`, `src/main/java/exchangeserver/command/Sequencer.java:60-64`).
2. `journal/JournalCodec.java` — `encode` (`:87-128`) and `decode` (`:130-164`) branches with `requireFieldCount`; update the format comment. A new record word that old builds cannot read is a format change: bump `FORMAT_VERSION` (`:41`) **[Inferred]**.
3. `core/Replay.java` — the verifier counts every record's sequence (`:114-129`); `feed` (`:72-99`) skips SKIP and compares CHECK; an engine command goes to `ledger.apply`.
4. `core/Ledger.java` — a branch in `apply` (`:42-55`) calling the engine; the digest update and `log.record` follow automatically.
5. `core/OutputLog.java` — `record` (`:58-76`) throws on an output it does not recognise: add a branch that numbers the private events your command produces.
6. `core/OutputDigest.java` — encode the new output kind (`:48-84`).
7. `core/Exchange.java` — stamp it in `process` (`:205-213`); add a `Request` type (Recipe 6) if callers hand it in.
8. `engine/` — `onExpire` returning a new `EngineOutput` (add it to `permits`).
9. `Rules.VERSION` bump (engine behaviour).
10. Tests: `JournalCodecTest` (round trip + corrupt cases, extra fields), `SequencerTest`, `ReplayTest`, `OutputLogTest`, `OutputDigestTest`, `ExchangeTest`.

These are `instanceof` chains, not exhaustive switches (Java 17), so the compiler will **not** list them for you: grep for `instanceof CancelAllCommand` to find every place the last record type was handled **[Verified]** (`Ledger.java:44-51`, `OutputLog.java:60-72`, `Exchange.java:205-213`).

**A new field on orders:** validate in `OrderRequest` (the only field-validation layer, D47), copy into `Order`, add to `encode`/`decode` (bump `FORMAT_VERSION`), to `RestingOrderView` and the snapshot format if it must survive compaction (`src/main/java/exchangeserver/core/Snapshot.java:64-116`, bump `Snapshot.FORMAT_VERSION`), to `OutputDigest` if outputs carry it, and to `Protocol` lines. Keep a NEW line ≤ 256 bytes (`Protocol.java:50`).

---

## Recipe 5 — Add a protocol message (e.g. `STATUS <orderId>`)

1. Session-local (like `PING`) or through the exchange? Anything reading state goes through the queue (D57; Recipe 6).
2. `gateway/ClientMessage.java` — a record; if it has a secret, override `toString` like `Login` does.
3. `gateway/Protocol.java` — a `case` in `parse` (`:60-100`) using `arity`/`number`; a formatter; update the comment block (it *is* the protocol documentation).
4. `gateway/ClientSession.java` — handle it in `handle` (`:211-266`). After the `LOGIN first` guard every exchange request:
   - passes `admit()` — the in-flight cap and rate limit (`:279-291`, D75);
   - uses a **callback** variant of `ExchangeService` wrapped in `tracked(...)` (`:421-432`), which counts it in flight so a closing session waits for it (D76);
   - reports refusals with `replyFailure` (`:413-419`).
   Callbacks run on the writer thread **after** the listener (D76) and must not block.
5. Add a `context(...)` label (`:268-276`) so `ERROR` lines say which request was refused.
6. Tests: `ProtocolTest.Parse` (including the malformed list), `ProtocolTest.Format`, a `TcpGatewayTest` case (log in with `login("alice")`, which sends the test secret).

---

## Recipe 6 — Add a read-only query through the queue

Reads are `Request`s that are never stamped or journalled **[Verified]** (`src/main/java/exchangeserver/core/Request.java:12-20`; `Exchange.java:231-234`).

1. Book/engine: a read method returning copies (`OrderBook.walk`/`restingOrders` return `RestingOrderView`s, `src/main/java/exchangeserver/book/OrderBook.java:149-165`).
2. `core/Request.java` — a record; validate arguments in its compact constructor, on the caller's thread (like `Snapshot` and `Resend`, `Request.java:35-55`).
3. `core/Exchange.java` — a branch in `read` (`:255-266`).
4. `core/ExchangeService.java` — a future variant via `handOff` (`:170-180`, D61) and a callback variant (`:186-209`).
5. Gateway: Recipe 5. No `Rules.VERSION` bump: reads do not change replay **[Inferred]**.

---

## Recipe 7 — Add a private event type

Private events are numbered per participant (oseq) and are what `RESEND` repeats **[Verified]** (`src/main/java/exchangeserver/core/PrivateEvent.java:12-32`, Spec D73).

1. `core/PrivateEvent.java` — a record in the sealed interface.
2. `core/OutputLog.java` — produce it in `record` (`:58-76`) with `add(new …(participant, next(participant), …))`; order inside one output matters: the requester's reply first, then others.
3. `gateway/Protocol.java` — format it in `lines` (`:145-166`) with the `oseq=` prefix and the `resend` marker.
4. No journal change: events are derived from commands, so replay rebuilds them **[Verified]** (`ReplayTest.Feed.feedsEveryCommandAndReportsTheCounters` checks a rebuilt oseq).
5. Tests: `OutputLogTest`, `ProtocolTest.Format`, a `TcpGatewayTest.Resend` case.

---

## Recipe 8 — Change settings, limits or CLI flags

Two settings records, both validated with `with…` copiers:
- `ExchangeService.Settings(queueCapacity, maxBatchSize, lingerNanos, clock, exchangeOptions)` with `Exchange.Options(checkInterval, outputRetention)` **[Verified]** (`src/main/java/exchangeserver/core/ExchangeService.java:59-92`, `Exchange.java:64-75`).
- `TcpGateway.Settings(bindAddress, port, credentials, tls, maxSessions, outboundCapacity, loginTimeout, idleTimeout, maxInFlight, ratePerSecond, burst)` **[Verified]** (`src/main/java/exchangeserver/gateway/TcpGateway.java:52-98`).

`Main` maps flags to both **[Verified]** (`src/main/java/exchangeserver/Main.java:71-85`, `:130-142`):

```java
// src/main/java/exchangeserver/Main.java:130-142 (trimmed)
gateway = new TcpGateway(TcpGateway.Settings.defaults(o.port, credentials)
        .withBindAddress(o.bind)
        .withTls(tls)
        .withTimeouts(Duration.ofSeconds(o.loginTimeout), Duration.ofSeconds(o.idleTimeout))
        .withFlowControl(o.maxInFlight, o.rate, o.burst));
gateway.startAccepting();
service = ExchangeService.start(o.journal, gateway, ExchangeService.Settings.defaults()
        .withExchangeOptions(new Exchange.Options(o.checkInterval, o.resendWindow)));
```

To add a flag: a field in `Main.Options` (`:49-65`), a `case` in the argument loop, wire it, update the header comment of `Main.java`, README "Run" and `CLAUDE.md` Commands. Batching defaults were chosen by measurement: re-run `BatchingBenchmark` before changing them (Spec §5, D52). Writer stats are read only after `stop()` in tests (Spec §10 #1).

---

## Recipe 9 — Add or rotate a participant's secret

Secrets live as salted PBKDF2-HMAC-SHA256 hashes in the users file **[Verified]** (`src/main/java/exchangeserver/gateway/Credentials.java:20-34`, D74).

1. `java -jar target/exchange-server2-2.0.0-SNAPSHOT.jar --users exchange.users --add-user alice` — asks twice on a terminal, or reads one line from stdin (a leading UTF-8 BOM from Windows PowerShell pipes is dropped, `Main.java:212-235`). Adding an existing name replaces its entry (rotation).
2. The running exchange reads the file at startup only: restart to apply **[Verified]** (`Main.java:124`, `Credentials.load`). Existing sessions stay logged in until they disconnect **[Inferred]**.
3. Rules for a secret: 8–64 printable ASCII characters, no spaces (`Credentials.SECRET`, `Credentials.java:38`).
4. In tests use `Credentials.of(map, 1_000)` — few iterations, because tests log in hundreds of times (`src/test/java/exchangeserver/gateway/TcpGatewayTest.java`).

---

## Recipe 10 — Enable TLS

1. Create a PKCS#12 keystore, e.g. `keytool -genkeypair -alias es2 -keyalg EC -groupname secp256r1 -dname CN=localhost -keystore server.p12 -storetype PKCS12` (the test `TcpGatewayTest.Security.tlsRoundTrip` does exactly this).
2. Start with `--tls-keystore server.p12` and the password in `ES2_TLS_PASSWORD` (never on the command line) **[Verified]** (`src/main/java/exchangeserver/gateway/Tls.java:52-58`, `Main.java:125-126`). Protocols: TLS 1.3 and 1.2 (`Tls.java:22`).
3. Clients: `ClientMain localhost 9000 --tls --truststore server.p12` with `ES2_TRUSTSTORE_PASSWORD`, or the JDK's default trust store for a CA-signed certificate.
4. If you `--bind` a non-loopback address without TLS, `Main` warns that secrets cross the network in clear text **[Verified]** (`Main.java:127-129`).

---

## Recipe 11 — Add a dependency

The production jar has **zero** runtime dependencies (`pom.xml:33-52`) **[Verified]**; TLS and PBKDF2 come from the JDK. A production dependency needs shading or a classpath (no `Class-Path` in the manifest) **[Inferred]**. Keep `-Xlint:all` clean; anything used inside the engine must stay deterministic (no clocks, randomness or hash-order iteration).

---

## Recipe 12 — Add a test

Conventions **[Verified]** (`CLAUDE.md` Tests):
- one test class per production class, `@Nested` per behaviour; every wait has a timeout;
- read engine state only through a future; read writer stats only after `stop()`;
- seams (package-private): `core.CommandInterceptor` (poison pill or `Gate`), `OrderBook.checkInvariants()`/`ordersAt`, `MatchingEngine.book()` and the `Limits` constructor, `Exchange.engine()`/`digest()`, `TcpGateway.participantsRegistered()`/`subscriberCount()`/`drainingSessions()`, `testing.MutableClock`;
- in gateway tests, the `Hold` listener in `TcpGatewayTest` holds the writer once to keep requests in flight;
- journals in `@TempDir`; handle release via `CommandJournal.openJournals()`; write journals with LF (`Files.writeString(…, String.join("\n", …))`) — `Files.write(path, lines)` uses CRLF on Windows and the decoder refuses it (D69);
- cross-process behaviour: spawn a child JVM as `CommandJournalTest.Exclusive.anotherProcessIsRefusedWhileTheJournalIsOpen` does.

---

## Recipe 13 — Run the suite on Linux

Docker, from the repo root **[Verified]** (run during the session; Spec §11):

```
docker run --rm -v "<repo>:/src:ro" eclipse-temurin:17 bash -c \
  "mkdir /w && (cd /src && tar --exclude=./target --exclude=./.git -cf - .) | tar -xf - -C /w \
   && cd /w && sed -i 's/\r$//' mvnw && chmod +x mvnw && ./mvnw -B test"
```

`.gitattributes` keeps `mvnw` LF in fresh checkouts; the `sed` covers working copies made before it existed. To test a signal from a script, enable job control (`set -m`): a non-interactive shell starts background jobs with SIGINT ignored.

---

## Key takeaways

- Three replay guards: `Rules.VERSION` (meaning), `FORMAT_VERSION` (layout), and `CHECK` digests (any output change). Cross a change by compacting with the old build.
- Check order in the engine: duplicate id → band → caps (only if resting) → FOK fillability → post-only cross; STP stops the match loop at the own order.
- A new command or output means `instanceof` branches in `Ledger`, `OutputLog`, `OutputDigest`, `Replay` and `Exchange` that the compiler does not list.
- Gateway requests go through `admit()` and `tracked(...)`; callbacks run after the listener.
- Secrets: `--add-user`, restart to apply; TLS: keystore + environment password.
- Every change ends with tests, `-Pmutation`, a D-number (next D78), docs, and `FULL-CONTEXT.md`.

## Common confusions

- **"A new `Reason` is just an enum constant."** It changes outputs (and the digest) and is a rule change: bump `Rules.VERSION`, and decide its `isReject()`.
- **"My refactor changed no rule, so old journals still replay."** If any output differs, the next CHECK stops replay as DIVERGED. Compact with the old build first.
- **"Compaction works across rules versions by itself."** It must run with the build that *wrote* the journal; the new build then adopts the empty journal.
- **"A new private event needs a journal record."** No: events are derived from commands and rebuilt by replay.
- **"Adding a user takes effect immediately."** The users file is read at startup.

## Questions to test yourself

1. You change `PRICE_BAND_PERCENT` to 15. How do you keep yesterday's book?
<details><summary>Answer</summary>Stop the old build and run <code>--compact</code> with it (snapshot + empty journal), then bump <code>Rules.VERSION</code> in the new build and start it: the empty journal adopts the new version and the book comes from the snapshot (D71).</details>

2. Where does a new "max notional" check go in `MatchingEngine.process`, and what may it change before rejecting?
<details><summary>Answer</summary>In the checks block (<code>MatchingEngine.java:182-213</code>) after the duplicate-id check (which consumes the id) — where its meaning fits relative to band and caps. Nothing but the id map may change before it rejects.</details>

3. You add an `EXPIRE` command. Name four places the compiler will not flag.
<details><summary>Answer</summary><code>Ledger.apply</code>, <code>OutputLog.record</code>, <code>OutputDigest.update</code>, <code>Exchange.process</code>'s stamping, <code>Replay.feed</code>, <code>JournalCodec.encode/decode</code>: all <code>instanceof</code> chains or string switches.</details>

4. Why does a new protocol request use `tracked(...)`?
<details><summary>Answer</summary>It counts the request in flight until its callback ran (after its lines went out), which the session limit uses and which lets a closing session wait for the reply (D75, D76).</details>

5. A refactor of `fillBest` changes nothing visible but swaps the order of two level updates in one output. What happens on restart with an old journal?
<details><summary>Answer</summary>The digest covers level updates in order, so it differs; replay stops at the next CHECK with <code>ReplayException(DIVERGED)</code> and <code>Main</code> advises compacting with the old build.</details>
