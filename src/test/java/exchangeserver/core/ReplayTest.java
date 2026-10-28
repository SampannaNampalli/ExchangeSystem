package exchangeserver.core;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.CheckCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;
import exchangeserver.domain.Side;
import exchangeserver.journal.CommandJournal;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplayTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");
    private static final int RETENTION = 100;

    @TempDir
    Path dir;

    private Path file() {
        return dir.resolve("r.journal");
    }

    private CommandJournal journalWith(Command... commands) throws IOException {
        CommandJournal j = CommandJournal.open(file(), Rules.VERSION);
        j.appendAll(List.of(commands));
        return j;
    }

    private Replay.Result run(CommandJournal j, long stopBefore, CommandInterceptor interceptor) throws IOException {
        return Replay.run(j, file(), stopBefore, interceptor, RETENTION);
    }

    private static NewOrderCommand buy(long seq, long id, Instant t) {
        return new NewOrderCommand(seq, t, id, OrderRequest.limit("a", "c" + id, Side.BUY, 100, 1));
    }

    // The digest a live run would have written after these commands.
    private static String digestAfter(Command... commands) {
        Ledger ledger = Ledger.from(Snapshot.empty(), RETENTION);
        for (Command c : commands) {
            ledger.apply(c);
        }
        return ledger.digest();
    }

    @Nested
    class Verify {

        @Test
        void emptyJournal() throws IOException {
            try (CommandJournal j = journalWith()) {
                Replay.Result r = run(j, Long.MAX_VALUE, CommandInterceptor.NONE);
                assertEquals(0, r.lastSequenceNumber());
                assertEquals(0, r.lastOrderId());
                assertNull(r.lastTimestamp());
                assertEquals(0, r.fed());
            }
        }

        @Test
        void gapInSequenceIsRefused() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(3, 2, T))) {
                IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> run(j, Long.MAX_VALUE, CommandInterceptor.NONE));
                assertTrue(e.getMessage().contains("gap"), e.getMessage());
            }
        }

        @Test
        void repeatedSequenceIsRefused() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(1, 2, T))) {
                assertThrows(IllegalStateException.class, () -> run(j, Long.MAX_VALUE, CommandInterceptor.NONE));
            }
        }

        @Test
        void backwardsTimestampIsRefused() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T.minusNanos(1)))) {
                assertThrows(IllegalStateException.class, () -> run(j, Long.MAX_VALUE, CommandInterceptor.NONE));
            }
        }

        @Test
        void nothingIsFedIfVerificationFails() throws IOException {
            // The gap is at the end: a single pass would already have changed the engine.
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), buy(4, 3, T))) {
                List<Command> seen = new ArrayList<>();
                assertThrows(IllegalStateException.class, () -> run(j, Long.MAX_VALUE, seen::add));
                assertTrue(seen.isEmpty());
            }
        }

        // D71: a journal that continues from a snapshot cannot be replayed without it.
        @Test
        void aBaseSequenceNeedsItsSnapshot() throws IOException {
            CommandJournal.writeFresh(file(), Rules.VERSION, 7);
            try (CommandJournal j = CommandJournal.open(file(), Rules.VERSION)) {
                IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> run(j, Long.MAX_VALUE, CommandInterceptor.NONE));
                assertTrue(e.getMessage().contains("snapshot-7"), e.getMessage());
            }
        }
    }

    @Nested
    class Feed {

        @Test
        void feedsEveryCommandAndReportsTheCounters() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T.plusSeconds(1)),
                    new CancelOrderCommand(3, T.plusSeconds(2), "a", 1))) {
                Replay.Result r = run(j, Long.MAX_VALUE, CommandInterceptor.NONE);
                assertEquals(3, r.lastSequenceNumber());
                assertEquals(2, r.lastOrderId(), "from the last NEW, not the cancel's target");
                assertEquals(T.plusSeconds(2), r.lastTimestamp());
                assertEquals(3, r.fed());
                assertEquals(1, r.ledger().engine().snapshot(10).bids().get(0).volume());
                assertEquals(3, r.ledger().log().lastOseq("a"), "D73: two order replies and a cancel reply, rebuilt by replay");
            }
        }

        @Test
        void skippedCommandsAreNotFedButTheirIdsCount() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), new SkipCommand(3, T, 2))) {
                List<Command> seen = new ArrayList<>();
                Replay.Result r = run(j, Long.MAX_VALUE, seen::add);
                assertEquals(List.of(buy(1, 1, T)), seen);
                assertEquals(Set.of(2L), r.skipped());
                assertEquals(2, r.lastOrderId(), "id 2 was issued, so it must never be issued again");
            }
        }

        @Test
        void stopsBeforeTheGivenSequence() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), buy(3, 3, T))) {
                List<Command> seen = new ArrayList<>();
                Replay.Result r = run(j, 3, seen::add);
                assertEquals(2, seen.size());
                assertEquals(3, r.lastSequenceNumber(), "verification still covers the whole journal");
            }
        }

        @Test
        void rebuildUsesTheGivenSkipSetAndDoesNotVerify() throws IOException {
            // A gap that run() would refuse: rebuild() trusts its caller (the poison rebuild) and does a single pass.
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(3, 2, T), buy(4, 3, T))) {
                List<Command> seen = new ArrayList<>();
                Ledger ledger = Replay.rebuild(j, file(), 4, Set.of(3L), seen::add, RETENTION);
                assertEquals(List.of(buy(1, 1, T)), seen);
                assertEquals(1, ledger.engine().snapshot(1).bids().get(0).volume());
            }
        }

        @Test
        void reusedOrderIdIsCaughtByTheEngine() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 5, T), buy(2, 5, T))) {
                ReplayException e = assertThrows(ReplayException.class, () -> run(j, Long.MAX_VALUE, CommandInterceptor.NONE));
                assertEquals(ReplayException.Kind.ENGINE_FAILURE, e.kind());
                assertEquals(2, e.sequenceNumber(), "L2: the operator is told which command to inspect");
            }
        }
    }

    // D70: CHECK records prove replay rebuilt the history the live run produced.
    @Nested
    class Check {

        @Test
        void aMatchingDigestPasses() throws IOException {
            String digest = digestAfter(buy(1, 1, T), buy(2, 2, T));
            try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), new CheckCommand(3, T, digest), buy(4, 3, T))) {
                Replay.Result r = run(j, Long.MAX_VALUE, CommandInterceptor.NONE);
                assertEquals(3, r.fed(), "the CHECK is compared, not fed");
            }
        }

        @Test
        void aDifferentDigestStopsReplayAsDivergence() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), new CheckCommand(2, T, "0123456789abcdef"))) {
                ReplayException e = assertThrows(ReplayException.class, () -> run(j, Long.MAX_VALUE, CommandInterceptor.NONE));
                assertEquals(ReplayException.Kind.DIVERGED, e.kind());
                assertEquals(2, e.sequenceNumber());
            }
        }

        @Test
        void aDifferentOutputChangesTheDigest() {
            // Same commands except one price: the outputs differ, so the digests must.
            NewOrderCommand other = new NewOrderCommand(2, T, 2, OrderRequest.limit("a", "c2", Side.BUY, 101, 1));
            assertTrue(!digestAfter(buy(1, 1, T), buy(2, 2, T)).equals(digestAfter(buy(1, 1, T), other)));
        }

        @Test
        void aCheckAtOrAfterStopBeforeIsNotCompared() throws IOException {
            try (CommandJournal j = journalWith(buy(1, 1, T), new CheckCommand(2, T, "0123456789abcdef"))) {
                assertEquals(1, run(j, 2, CommandInterceptor.NONE).fed());
            }
        }
    }
}
