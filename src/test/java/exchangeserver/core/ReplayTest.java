package exchangeserver.core;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;
import exchangeserver.domain.Side;
import exchangeserver.engine.MatchingEngine;
import exchangeserver.journal.CommandJournal;
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

    @TempDir
    Path dir;

    private CommandJournal journalWith(Command... commands) throws IOException {
        CommandJournal j = CommandJournal.open(dir.resolve("r.journal"), Rules.VERSION);
        j.appendAll(List.of(commands));
        return j;
    }

    private static NewOrderCommand buy(long seq, long id, Instant t) {
        return new NewOrderCommand(seq, t, id, OrderRequest.limit("a", "c" + id, Side.BUY, 100, 1));
    }

    @Test
    void emptyJournal() throws IOException {
        try (CommandJournal j = journalWith()) {
            Replay.Result r = Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, CommandInterceptor.NONE);
            assertEquals(0, r.lastSequenceNumber());
            assertEquals(0, r.lastOrderId());
            assertNull(r.lastTimestamp());
            assertEquals(0, r.fed());
        }
    }

    @Test
    void feedsEveryCommandAndReportsTheCounters() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T.plusSeconds(1)),
                new CancelOrderCommand(3, T.plusSeconds(2), "a", 1))) {
            MatchingEngine engine = new MatchingEngine();
            Replay.Result r = Replay.run(j, engine, Long.MAX_VALUE, CommandInterceptor.NONE);
            assertEquals(3, r.lastSequenceNumber());
            assertEquals(2, r.lastOrderId(), "from the last NEW, not the cancel's target");
            assertEquals(T.plusSeconds(2), r.lastTimestamp());
            assertEquals(3, r.fed());
            assertEquals(1, engine.snapshot(10).bids().get(0).volume());
        }
    }

    @Test
    void skippedCommandsAreNotFedButTheirIdsCount() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), new SkipCommand(3, T, 2))) {
            List<Command> seen = new ArrayList<>();
            Replay.Result r = Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, seen::add);
            assertEquals(List.of(buy(1, 1, T)), seen);
            assertEquals(Set.of(2L), r.skipped());
            assertEquals(2, r.lastOrderId(), "id 2 was issued, so it must never be issued again");
        }
    }

    @Test
    void stopsBeforeTheGivenSequence() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), buy(3, 3, T))) {
            List<Command> seen = new ArrayList<>();
            Replay.Result r = Replay.run(j, new MatchingEngine(), 3, seen::add);
            assertEquals(2, seen.size());
            assertEquals(3, r.lastSequenceNumber(), "verification still covers the whole journal");
        }
    }

    @Test
    void feedAloneUsesTheGivenSkipSetAndDoesNotVerify() throws IOException {
        // A gap that run() would refuse: feed() trusts its caller (the poison rebuild) and does a single pass.
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(3, 2, T), buy(4, 3, T))) {
            List<Command> seen = new ArrayList<>();
            long fed = Replay.feed(j, new MatchingEngine(), 4, Set.of(3L), seen::add);
            assertEquals(1, fed);
            assertEquals(List.of(buy(1, 1, T)), seen);
        }
    }

    @Test
    void gapInSequenceIsRefused() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(3, 2, T))) {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, CommandInterceptor.NONE));
            assertTrue(e.getMessage().contains("gap"), e.getMessage());
        }
    }

    @Test
    void repeatedSequenceIsRefused() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(1, 2, T))) {
            assertThrows(IllegalStateException.class,
                    () -> Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, CommandInterceptor.NONE));
        }
    }

    @Test
    void backwardsTimestampIsRefused() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T.minusNanos(1)))) {
            assertThrows(IllegalStateException.class,
                    () -> Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, CommandInterceptor.NONE));
        }
    }

    @Test
    void nothingIsFedIfVerificationFails() throws IOException {
        // The gap is at the end: a single pass would already have changed the engine.
        try (CommandJournal j = journalWith(buy(1, 1, T), buy(2, 2, T), buy(4, 3, T))) {
            List<Command> seen = new ArrayList<>();
            assertThrows(IllegalStateException.class, () -> Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, seen::add));
            assertTrue(seen.isEmpty());
        }
    }

    @Test
    void reusedOrderIdIsCaughtByTheEngine() throws IOException {
        try (CommandJournal j = journalWith(buy(1, 5, T), buy(2, 5, T))) {
            assertThrows(IllegalStateException.class,
                    () -> Replay.run(j, new MatchingEngine(), Long.MAX_VALUE, CommandInterceptor.NONE));
        }
    }
}
