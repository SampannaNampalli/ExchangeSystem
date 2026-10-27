package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.book.CancelStatus;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.OrderStatus;
import exchangeserver.engine.Reason;
import exchangeserver.engine.Response;
import exchangeserver.journal.CommandJournal;
import exchangeserver.testing.MutableClock;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExchangeTest {

    @TempDir
    Path dir;

    private final MutableClock clock = MutableClock.at("2026-01-01T10:00:00Z");

    private Path journal() {
        return dir.resolve("exchange.journal");
    }

    private Exchange open() throws IOException {
        return Exchange.open(journal(), clock);
    }

    private static OrderRequest limit(String p, String c, Side side, long price, long qty) {
        return OrderRequest.limit(p, c, side, price, qty);
    }

    // Throws on any order whose client order id starts with "POISON": a stand-in for an engine bug.
    private static final CommandInterceptor POISON = command -> {
        if (command instanceof NewOrderCommand n && n.request().getClientOrderId().startsWith("POISON")) {
            throw new IllegalStateException("simulated engine bug");
        }
    };

    private List<String> journalLines() throws IOException {
        return Files.readAllLines(journal(), UTF_8);
    }

    @Nested
    class LivePath {

        @Test
        void submitAndCancel() throws IOException {
            try (Exchange ex = open()) {
                Response r = ex.submit(limit("a", "c1", Side.BUY, 100, 10));
                assertEquals(1, r.orderId());
                assertEquals(OrderStatus.RESTED, r.status());
                assertEquals(CancelResult.cancelled(1, 10), ex.cancel("a", 1));
                assertEquals(2, ex.lastSequenceNumber());
            }
        }

        @Test
        void visibleImpliesDurable() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "c1", Side.BUY, 100, 10));
                // The response exists, so the record must already be in the file (written and forced).
                List<String> lines = journalLines();
                assertEquals(2, lines.size());
                assertTrue(lines.get(1).startsWith("NEW,1,2026-01-01T10:00:00Z,1,a,c1,BUY,LIMIT,10,100"), lines.get(1));
            }
        }

        @Test
        void stateDependentRejectsAreJournalled() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "X", Side.BUY, 100, 1));
                assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, ex.submit(limit("a", "X", Side.BUY, 100, 1)).reason());
                assertEquals(CancelStatus.UNKNOWN_ORDER, ex.cancel("a", 99).status());
            }
            assertEquals(4, journalLines().size(), "header + 3 commands, rejects included (D13)");
        }

        @Test
        void invalidCancelIsRefusedWithoutUsingASequenceNumber() throws IOException {
            try (Exchange ex = open()) {
                assertThrows(IllegalArgumentException.class, () -> ex.cancel("not valid", 1));
                assertEquals(0, ex.lastSequenceNumber());
            }
            assertEquals(1, journalLines().size());
        }

        @Test
        void snapshotIsNotJournalled() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "c1", Side.BUY, 100, 10));
                BookSnapshot s = ex.snapshot(5);
                assertEquals(1, s.sequenceNumber());
                assertEquals(1, ex.lastSequenceNumber());
            }
            assertEquals(2, journalLines().size());
        }
    }

    @Nested
    class Restart {

        @Test
        void rebuildsTheIdenticalBookAndContinuesTheCounters() throws IOException {
            BookSnapshot before;
            try (Exchange ex = open()) {
                randomTraffic(ex, 7, 500);
                before = ex.snapshot(Integer.MAX_VALUE);
            }
            clock.advance(Duration.ofMinutes(5));
            try (Exchange ex = open()) {
                assertEquals(before, ex.snapshot(Integer.MAX_VALUE));
                long last = ex.lastSequenceNumber();
                Response r = ex.submit(limit("z", "new", Side.BUY, 1, 1));
                assertEquals(last + 1, ex.lastSequenceNumber());
                assertTrue(r.orderId() > 0);
            }
        }

        @Test
        void timePriorityAndPartialFillsSurvive() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("s1", "a", Side.SELL, 100, 5));      // 1
                ex.submit(limit("s2", "b", Side.SELL, 100, 5));      // 2
                ex.submit(limit("b", "c", Side.BUY, 100, 3));        // fills 3 of order 1
            }
            try (Exchange ex = open()) {
                Response r = ex.submit(limit("b", "d", Side.BUY, 100, 4));
                // Order 1 has 2 left and still has priority over order 2.
                assertEquals(2, r.trades().get(0).quantity());
                assertEquals(1, r.trades().get(0).sellOrderId());
                assertEquals(2, r.trades().get(1).sellOrderId());
            }
        }

        @Test
        void nextOrderIdComesFromTheLastNewOrderNotTheLastRecord() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 100, 1));        // id 1
                ex.submit(limit("a", "2", Side.BUY, 100, 1));        // id 2
                ex.cancel("a", 1);                                   // last record targets id 1
            }
            try (Exchange ex = open()) {
                assertEquals(3, ex.submit(limit("a", "3", Side.BUY, 100, 1)).orderId());
            }
        }

        @Test
        void clientOrderIdsSurviveSoAResendAfterRestartIsCaught() throws IOException {
            long original;
            try (Exchange ex = open()) {
                original = ex.submit(limit("a", "X", Side.BUY, 100, 1)).orderId();
            }
            try (Exchange ex = open()) {
                Response resend = ex.submit(limit("a", "X", Side.BUY, 100, 1));
                assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, resend.reason());
                assertEquals(original, resend.orderId());
            }
        }

        @Test
        void timestampsNeverGoBackAcrossARestart() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 100, 1));
            }
            clock.advance(Duration.ofHours(-1));                    // the machine's clock went back
            try (Exchange ex = open()) {
                ex.submit(limit("a", "2", Side.BUY, 100, 1));
            }
            try (Exchange ex = open()) {
                assertEquals(2, ex.replayedOnOpen(), "replay verified the timestamps did not decrease");
            }
        }

        @Test
        void tornTailFromACrashIsCutAndReported() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 100, 1));
            }
            Files.writeString(journal(), "NEW,2,2026-01", UTF_8, StandardOpenOption.APPEND);
            try (Exchange ex = open()) {
                assertEquals(13, ex.truncatedBytesOnOpen());
                assertEquals(1, ex.lastSequenceNumber());
                assertEquals(2, ex.submit(limit("a", "2", Side.BUY, 100, 1)).orderId());
            }
        }

        @Test
        void journalFromOtherRulesIsRefused() throws IOException {
            Files.writeString(journal(), "HEADER,2,999\n", UTF_8);
            assertThrows(IllegalStateException.class, ExchangeTest.this::open);
        }

        @Test
        void failedReplayReleasesTheJournal() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 100, 1));
            }
            Files.writeString(journal(), "NEW,garbage\n", UTF_8, StandardOpenOption.APPEND);
            int open = CommandJournal.openJournals();
            assertThrows(IllegalStateException.class, ExchangeTest.this::open);
            assertEquals(open, CommandJournal.openJournals(), "D43: a failed open closes the journal");
        }

        @Test
        void closeReleasesTheJournal() throws IOException {
            int open = CommandJournal.openJournals();
            Exchange ex = open();
            assertEquals(open + 1, CommandJournal.openJournals());
            ex.close();
            assertEquals(open, CommandJournal.openJournals());
        }

        private void randomTraffic(Exchange ex, long seed, int n) throws IOException {
            Random random = new Random(seed);
            for (int i = 0; i < n; i++) {
                clock.advance(Duration.ofMillis(random.nextInt(3)));
                if (i % 4 == 3) {
                    ex.cancel("p" + random.nextInt(3), 1 + random.nextInt(i));
                } else {
                    Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
                    ex.submit(limit("p" + random.nextInt(3), "c" + i, side, 97 + random.nextInt(7), 1 + random.nextInt(9)));
                }
            }
        }
    }

    @Nested
    class PoisonPill {

        @Test
        void failingCommandIsQuarantinedAndTheExchangeCarriesOn() throws IOException {
            try (Exchange ex = Exchange.open(journal(), clock, POISON)) {
                ex.submit(limit("a", "ok1", Side.SELL, 100, 5));                 // seq 1
                ExchangeException e = assertThrows(ExchangeException.class,
                        () -> ex.submit(limit("a", "POISON", Side.BUY, 100, 5)));  // seq 2
                assertEquals(ExchangeException.Reason.INTERNAL_ERROR, e.getReason());
                assertEquals(Set.of(2L), ex.quarantined());
                Response after = ex.submit(limit("b", "ok2", Side.BUY, 100, 5)); // seq 4 (3 is the SKIP)
                assertEquals(OrderStatus.FILLED, after.status(), "the resting sell is still there");
            }
            assertTrue(journalLines().get(3).startsWith("SKIP,3,"), journalLines().toString());
        }

        @Test
        void restartAfterQuarantineSkipsThePoison() throws IOException {
            BookSnapshot before;
            try (Exchange ex = Exchange.open(journal(), clock, POISON)) {
                ex.submit(limit("a", "ok1", Side.SELL, 100, 5));
                assertThrows(ExchangeException.class, () -> ex.submit(limit("a", "POISON", Side.BUY, 101, 5)));
                ex.submit(limit("b", "ok2", Side.BUY, 99, 5));
                before = ex.snapshot(10);
            }
            // The interceptor would throw again if replay fed the poison.
            try (Exchange ex = Exchange.open(journal(), clock, POISON)) {
                assertEquals(before, ex.snapshot(10));
                assertEquals(Set.of(2L), ex.quarantined());
            }
        }

        @Test
        void poisonInTheMiddleOfABatchDoesNotDisturbTheOthers() throws IOException {
            try (Exchange ex = Exchange.open(journal(), clock, POISON)) {
                Pending<Response> a = new Pending<>(new Request.Submit(limit("a", "a", Side.SELL, 100, 5)));
                Pending<Response> bad = new Pending<>(new Request.Submit(limit("a", "POISON", Side.BUY, 100, 5)));
                Pending<Response> b = new Pending<>(new Request.Submit(limit("b", "b", Side.BUY, 100, 2)));
                List<EngineOutput> outputs = new ArrayList<>();
                ex.process(List.of(a, bad, b), (output, events) -> outputs.add(output));

                assertEquals(OrderStatus.RESTED, a.result.join().status());
                CompletionException e = assertThrows(CompletionException.class, bad.result::join);
                assertEquals(ExchangeException.Reason.INTERNAL_ERROR, ((ExchangeException) e.getCause()).getReason());
                Response rb = b.result.join();
                assertEquals(OrderStatus.FILLED, rb.status());
                assertEquals(1, rb.trades().get(0).sellOrderId(), "matched the order before the poison");
                assertEquals(List.of(1L, 3L), outputs.stream().map(EngineOutput::sequenceNumber).toList(),
                        "the listener never hears about the poison");
                assertEquals(3, ex.engine().snapshot(1).asks().get(0).volume());
            }
        }

        @Test
        void unquarantinedPoisonStopsStartupUntilAnOperatorQuarantinesIt() throws IOException {
            // A poison written by an older build (or before a crash) is in the journal without a SKIP.
            try (Exchange ex = open()) {
                ex.submit(limit("a", "ok", Side.SELL, 100, 5));
                ex.submit(limit("a", "POISON", Side.BUY, 90, 5));                // seq 2: fine for this build
            }
            assertThrows(IllegalStateException.class, () -> Exchange.open(journal(), clock, POISON));

            Exchange.quarantine(journal(), 2, clock);
            try (Exchange ex = Exchange.open(journal(), clock, POISON)) {
                assertEquals(Set.of(2L), ex.quarantined());
                assertEquals(3, ex.lastSequenceNumber());
                assertTrue(ex.snapshot(10).bids().isEmpty());
            }
        }

        @Test
        void quarantineToolRefusesANonexistentCommand() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "ok", Side.SELL, 100, 5));
            }
            assertThrows(IllegalArgumentException.class, () -> Exchange.quarantine(journal(), 7, clock));
            assertEquals(2, journalLines().size());
        }

        @Test
        void quarantineToolRefusesToSkipASkip() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "ok", Side.SELL, 100, 5));
            }
            Exchange.quarantine(journal(), 1, clock);                           // writes SKIP as seq 2
            assertThrows(IllegalArgumentException.class, () -> Exchange.quarantine(journal(), 2, clock));
            assertEquals(3, journalLines().size());
        }

        // D65: the tool used to append with its own sequence numbers next to the running exchange's.
        @Test
        void quarantineToolRefusesWhileTheExchangeIsRunning() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "ok", Side.SELL, 100, 5));
                assertThrows(IllegalStateException.class, () -> Exchange.quarantine(journal(), 1, clock));
                ex.submit(limit("a", "ok2", Side.SELL, 100, 5));
            }
            assertEquals(3, journalLines().size(), "header and two orders, no SKIP");
            try (Exchange ex = open()) {
                assertEquals(2, ex.lastSequenceNumber());
            }
        }

        @Test
        void errorsAreNotQuarantinedTheyPropagate() throws IOException {
            CommandInterceptor fatal = c -> {
                throw new AssertionError("not a RuntimeException");
            };
            try (Exchange ex = Exchange.open(journal(), clock, fatal)) {
                assertThrows(AssertionError.class, () -> ex.submit(limit("a", "x", Side.SELL, 100, 5)));
                assertTrue(ex.quarantined().isEmpty());
            }
        }
    }

    // D70 (M5): the live run writes the digest of its outputs; replay must reproduce it.
    @Nested
    class Checkpoints {

        private Exchange openWithCheckEvery(int interval) throws IOException {
            return Exchange.open(journal(), clock, CommandInterceptor.NONE, new Exchange.Options(interval, 100));
        }

        @Test
        void aCheckIsWrittenEveryIntervalAndReplayAcceptsIt() throws IOException {
            String digest;
            try (Exchange ex = openWithCheckEvery(2)) {
                ex.submit(limit("a", "1", Side.SELL, 100, 5));
                ex.submit(limit("b", "2", Side.BUY, 100, 2));
                ex.submit(limit("a", "3", Side.SELL, 101, 5));          // stamped after a CHECK of the first two
                digest = ex.digest();
            }
            List<String> lines = journalLines();
            assertTrue(lines.get(3).startsWith("CHECK,3,"), lines.toString());
            try (Exchange ex = openWithCheckEvery(2)) {
                assertEquals(digest, ex.digest(), "replay rebuilt the same outputs");
                assertEquals(4, ex.lastSequenceNumber());
            }
        }

        @Test
        void aChangedHistoryIsCaughtAtTheNextCheck() throws IOException {
            try (Exchange ex = openWithCheckEvery(1)) {
                ex.submit(limit("a", "1", Side.SELL, 100, 5));
                ex.submit(limit("b", "2", Side.BUY, 100, 2));           // CHECK 2 covers command 1
            }
            // Stand-in for a code change that alters behaviour: make the engine see different input for command 1.
            CommandInterceptor differentBehaviour = c -> { };
            List<String> lines = journalLines();
            String tampered = lines.get(1).replace(",SELL,LIMIT,5,100,", ",SELL,LIMIT,4,100,");
            String body = tampered.substring(0, tampered.lastIndexOf(','));
            lines.set(1, body + "," + exchangeserver.journal.Checksums.crcHex(body));
            Files.writeString(journal(), String.join("\n", lines) + "\n", UTF_8);       // LF: the journal's format
            ReplayException e = assertThrows(ReplayException.class,
                    () -> Exchange.open(journal(), clock, differentBehaviour, new Exchange.Options(1, 100)));
            assertEquals(ReplayException.Kind.DIVERGED, e.kind());
            assertEquals(2, e.sequenceNumber());
        }
    }

    // D71 (H3): a snapshot plus an empty journal replaces the whole history.
    @Nested
    class Compaction {

        @Test
        void restartAfterCompactionRebuildsTheSameState() throws IOException {
            BookSnapshot before;
            long lastSeq;
            try (Exchange ex = open()) {
                ex.submit(limit("a", "a1", Side.SELL, 100, 5));
                ex.submit(limit("b", "b1", Side.SELL, 100, 3));
                ex.submit(limit("x", "x1", Side.BUY, 100, 2));
                ex.submit(limit("c", "c1", Side.BUY, 95, 4));
                before = ex.snapshot(10);
                lastSeq = ex.lastSequenceNumber();
            }
            Exchange.Compaction c = Exchange.compact(journal());
            assertEquals(lastSeq, c.lastSequence());
            assertEquals(3, c.restingOrders());
            assertTrue(Files.exists(c.snapshot()));
            assertTrue(Files.exists(c.archive()));
            assertEquals(1, journalLines().size(), "only the header is left");

            try (Exchange ex = open()) {
                assertEquals(before, ex.snapshot(10));
                assertEquals(lastSeq, ex.recovery().baseSequence());
                assertEquals(0, ex.recovery().replayed());
                Response r = ex.submit(limit("d", "d1", Side.BUY, 100, 4));
                assertEquals(List.of(1L, 2L), r.trades().stream().map(t -> t.sellOrderId()).toList(), "time priority survived");
                assertEquals(lastSeq + 1, ex.lastSequenceNumber());
            }
        }

        @Test
        void clientOrderIdsAreScopedToTheJournalExceptForRestingOrders() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "gone", Side.BUY, 90, 1));
                ex.cancel("a", 1);
                ex.submit(limit("a", "rests", Side.BUY, 90, 1));
            }
            Exchange.compact(journal());
            try (Exchange ex = open()) {
                assertEquals(OrderStatus.RESTED, ex.submit(limit("a", "gone", Side.BUY, 91, 1)).status(), "a new day");
                assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, ex.submit(limit("a", "rests", Side.BUY, 92, 1)).reason(),
                        "a resting order keeps its id");
            }
        }

        @Test
        void outputNumberingContinuesAcrossACompaction() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
                ex.submit(limit("a", "2", Side.BUY, 90, 1));
            }
            Exchange.compact(journal());
            try (Exchange ex = open()) {
                ex.submit(limit("a", "3", Side.BUY, 90, 1));
                ResendResult r = ex.resend("a", 1);
                assertEquals(3, r.lastOseq());
                assertEquals(3, r.firstAvailable(), "a snapshot keeps the numbers, not the events");
            }
        }

        @Test
        void aSecondCompactionContinuesTheChainAndNothingToCompactIsReported() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
            }
            Exchange.compact(journal());
            assertEquals(null, Exchange.compact(journal()), "no records since the snapshot");
            try (Exchange ex = open()) {
                ex.submit(limit("a", "2", Side.BUY, 91, 1));
            }
            Exchange.Compaction second = Exchange.compact(journal());
            assertEquals(2, second.lastSequence());
            try (Exchange ex = open()) {
                assertEquals(2, ex.snapshot(10).bids().size());
            }
        }

        @Test
        void aCrashAfterWritingTheSnapshotLeavesTheOldJournalInCharge() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
                ex.submit(limit("a", "2", Side.BUY, 91, 1));
            }
            // As if compaction wrote the snapshot and crashed before replacing the journal.
            Files.writeString(Snapshot.path(journal(), 2), "garbage", UTF_8);
            try (Exchange ex = open()) {
                assertEquals(2, ex.snapshot(10).bids().size());
                assertEquals(0, ex.recovery().baseSequence(), "the journal names its own base: none");
            }
        }

        @Test
        void aRulesChangeIsCrossedWithAnEmptyCompactedJournal() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
            }
            Exchange.compact(journal());
            // As if the old build (rules VERSION - 1) had compacted: its empty journal carries its own version.
            CommandJournal.writeFresh(journal(), exchangeserver.domain.Rules.VERSION - 1, 1);
            try (Exchange ex = open()) {
                assertEquals(exchangeserver.domain.Rules.VERSION - 1, ex.recovery().adoptedRulesFrom());
                assertEquals(1, ex.snapshot(10).bids().size(), "the book came from the snapshot");
            }
        }

        @Test
        void aPoisonAfterACompactionRebuildsFromTheSnapshot() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "ok", Side.SELL, 100, 5));
            }
            Exchange.compact(journal());
            try (Exchange ex = Exchange.open(journal(), clock, POISON)) {
                assertThrows(ExchangeException.class, () -> ex.submit(limit("b", "POISON", Side.BUY, 100, 5)));
                assertEquals(OrderStatus.FILLED, ex.submit(limit("b", "b1", Side.BUY, 100, 5)).status(),
                        "the rebuilt ledger still has the snapshot's resting order");
            }
        }

        @Test
        void aCorruptSnapshotStopsStartup() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
            }
            Exchange.Compaction c = Exchange.compact(journal());
            List<String> lines = Files.readAllLines(c.snapshot(), UTF_8);
            lines.set(1, lines.get(1).replace(",90,", ",80,"));
            Files.write(c.snapshot(), lines, UTF_8);
            IllegalStateException e = assertThrows(IllegalStateException.class, this::reopen);
            assertTrue(e.getMessage().contains("Corrupt snapshot"), e.getMessage());
        }

        private void reopen() throws IOException {
            open().close();
        }
    }

    // L1, L3: what the operator is told and allowed.
    @Nested
    class Operations {

        @Test
        void recoveryReportsWhatItDid() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
                ex.submit(limit("a", "2", Side.BUY, 91, 1));
            }
            Files.writeString(journal(), "NEW,3,2026", UTF_8, StandardOpenOption.APPEND);
            try (Exchange ex = open()) {
                Exchange.Recovery r = ex.recovery();
                assertEquals(0, r.baseSequence());
                assertEquals(2, r.lastSequence());
                assertEquals(2, r.replayed());
                assertEquals(10, r.truncatedBytes());
                assertTrue(r.quarantined().isEmpty());
            }
        }

        @Test
        void quarantineRefusesACommandAlreadyQuarantined() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "ok", Side.SELL, 100, 5));
            }
            Exchange.quarantine(journal(), 1, clock);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Exchange.quarantine(journal(), 1, clock));
            assertTrue(e.getMessage().contains("already quarantined"), e.getMessage());
            assertEquals(3, journalLines().size());
        }

        @Test
        void cancelAllThroughTheLivePathIsJournalledAndReplayed() throws IOException {
            try (Exchange ex = open()) {
                ex.submit(limit("a", "1", Side.BUY, 90, 1));
                ex.submit(limit("a", "2", Side.SELL, 110, 1));
                assertEquals(2, ex.cancelAll("a").size());
            }
            assertTrue(journalLines().get(3).startsWith("CANCEL_ALL,3,"));
            try (Exchange ex = open()) {
                assertTrue(ex.snapshot(10).bids().isEmpty() && ex.snapshot(10).asks().isEmpty());
                assertEquals(4, ex.resend("a", 1).lastOseq(), "two replies and two cancel replies, rebuilt");
            }
        }
    }

    @Test
    void interceptorSeesEveryCommandLiveInOrder() throws IOException {
        List<Long> seen = new ArrayList<>();
        CommandInterceptor record = c -> seen.add(c.sequenceNumber());
        try (Exchange ex = Exchange.open(journal(), clock, record)) {
            ex.submit(limit("a", "1", Side.SELL, 100, 5));
            ex.cancel("a", 1);
            ex.snapshot(1);
            ex.submit(limit("a", "2", Side.SELL, 100, 5));
        }
        assertEquals(List.of(1L, 2L, 3L), seen, "reads are never stamped or journalled");
    }
}
