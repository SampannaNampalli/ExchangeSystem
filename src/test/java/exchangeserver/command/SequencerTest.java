package exchangeserver.command;

import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.testing.MutableClock;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SequencerTest {

    private static final OrderRequest REQUEST = OrderRequest.limit("a", "c", Side.BUY, 100, 1);

    private final MutableClock clock = MutableClock.at("2026-01-01T10:00:00Z");
    private final Sequencer sequencer = new Sequencer(clock);

    @Nested
    class Stamping {

        @Test
        void sequenceNumbersAreContiguousAcrossAllKinds() {
            assertEquals(1, sequencer.stampNewOrder(REQUEST).sequenceNumber());
            assertEquals(2, sequencer.stampCancel("a", 1).sequenceNumber());
            assertEquals(3, sequencer.stampSkip(2).sequenceNumber());
            assertEquals(4, sequencer.stampNewOrder(REQUEST).sequenceNumber());
        }

        @Test
        void onlyNewOrdersUseUpOrderIds() {
            assertEquals(1, sequencer.stampNewOrder(REQUEST).orderId());
            sequencer.stampCancel("a", 1);
            sequencer.stampSkip(1);
            assertEquals(2, sequencer.stampNewOrder(REQUEST).orderId());
        }

        @Test
        void cancelCarriesItsTargetAndParticipant() {
            CancelOrderCommand c = sequencer.stampCancel("bob", 42);
            assertEquals(42, c.orderId());
            assertEquals("bob", c.participant());
        }

        @Test
        void timestampComesFromTheClock() {
            clock.set(Instant.parse("2026-05-05T05:05:05.123456789Z"));
            assertEquals(Instant.parse("2026-05-05T05:05:05.123456789Z"), sequencer.stampNewOrder(REQUEST).timestamp());
        }

        @Test
        void requestIsKeptAsIs() {
            assertEquals(REQUEST, sequencer.stampNewOrder(REQUEST).request());
        }
    }

    @Nested
    class InvalidInputUsesUpNothing {

        @Test
        void nullRequest() {
            assertThrows(NullPointerException.class, () -> sequencer.stampNewOrder(null));
            assertEquals(1, sequencer.nextSequenceNumber());
            assertEquals(1, sequencer.nextOrderId());
        }

        @Test
        void invalidCancel() {
            assertThrows(IllegalArgumentException.class, () -> sequencer.stampCancel("bad participant", 1));
            assertThrows(IllegalArgumentException.class, () -> sequencer.stampCancel("a", 0));
            assertEquals(1, sequencer.nextSequenceNumber());
        }

        @Test
        void invalidSkip() {
            assertThrows(IllegalArgumentException.class, () -> sequencer.stampSkip(1));    // would skip itself
            assertEquals(1, sequencer.nextSequenceNumber());
        }
    }

    @Nested
    class Clock {

        @Test
        void backwardsStepReusesThePreviousTimestamp() {
            Instant first = sequencer.stampNewOrder(REQUEST).timestamp();
            clock.advance(Duration.ofSeconds(-30));
            assertEquals(first, sequencer.stampNewOrder(REQUEST).timestamp());
            assertEquals(first, sequencer.stampCancel("a", 1).timestamp());
        }

        @Test
        void resumesNormallyOnceTheClockCatchesUp() {
            Instant first = sequencer.stampNewOrder(REQUEST).timestamp();
            clock.advance(Duration.ofSeconds(-30));
            sequencer.stampNewOrder(REQUEST);
            clock.advance(Duration.ofSeconds(31));
            assertEquals(first.plusSeconds(1), sequencer.stampNewOrder(REQUEST).timestamp());
        }

        @Test
        void equalTimestampsAreAllowed() {
            Instant a = sequencer.stampNewOrder(REQUEST).timestamp();
            assertEquals(a, sequencer.stampNewOrder(REQUEST).timestamp());
        }
    }

    @Nested
    class Resume {

        @Test
        void continuesAfterTheLastValues() {
            Sequencer s = Sequencer.resume(clock, 10, 7, Instant.parse("2026-01-01T10:00:00Z"));
            NewOrderCommand n = s.stampNewOrder(REQUEST);
            assertEquals(11, n.sequenceNumber());
            assertEquals(8, n.orderId());
        }

        @Test
        void neverStampsBeforeTheLastJournalledTimestamp() {
            Instant last = Instant.parse("2030-01-01T00:00:00Z");      // the clock is behind the journal
            Sequencer s = Sequencer.resume(clock, 3, 3, last);
            assertEquals(last, s.stampNewOrder(REQUEST).timestamp());
        }

        @Test
        void emptyHistoryResumesFromOne() {
            Sequencer s = Sequencer.resume(clock, 0, 0, null);
            assertEquals(1, s.stampNewOrder(REQUEST).sequenceNumber());
        }

        @Test
        void negativeCountersAreRejected() {
            assertThrows(IllegalArgumentException.class, () -> Sequencer.resume(clock, -1, 0, null));
            assertThrows(IllegalArgumentException.class, () -> Sequencer.resume(clock, 0, -1, null));
        }
    }

    @Nested
    class CommandValidation {

        private final Instant t = Instant.parse("2026-01-01T10:00:00Z");

        @Test
        void newOrderCommand() {
            assertThrows(IllegalArgumentException.class, () -> new NewOrderCommand(0, t, 1, REQUEST));
            assertThrows(IllegalArgumentException.class, () -> new NewOrderCommand(1, t, 0, REQUEST));
            assertThrows(NullPointerException.class, () -> new NewOrderCommand(1, null, 1, REQUEST));
            assertThrows(NullPointerException.class, () -> new NewOrderCommand(1, t, 1, null));
        }

        @Test
        void cancelOrderCommand() {
            assertThrows(IllegalArgumentException.class, () -> new CancelOrderCommand(0, t, "a", 1));
            assertThrows(IllegalArgumentException.class, () -> new CancelOrderCommand(1, t, "a", -1));
            assertThrows(IllegalArgumentException.class, () -> new CancelOrderCommand(1, t, null, 1));
            assertThrows(NullPointerException.class, () -> new CancelOrderCommand(1, null, "a", 1));
        }

        @Test
        void cancelAllCommand() {
            assertThrows(IllegalArgumentException.class, () -> new CancelAllCommand(0, t, "a"));
            assertThrows(IllegalArgumentException.class, () -> new CancelAllCommand(1, t, "a b"));
            assertThrows(NullPointerException.class, () -> new CancelAllCommand(1, null, "a"));
        }

        @Test
        void checkCommandNeedsSixteenHexDigits() {
            new CheckCommand(1, t, "0123456789abcdef");
            assertThrows(IllegalArgumentException.class, () -> new CheckCommand(1, t, "0123456789ABCDEF"));
            assertThrows(IllegalArgumentException.class, () -> new CheckCommand(1, t, "0123"));
            assertThrows(IllegalArgumentException.class, () -> new CheckCommand(0, t, "0123456789abcdef"));
        }

        @Test
        void cancelAllAndCheckUseSequenceNumbersButNoOrderIds() {
            Sequencer s = new Sequencer(java.time.Clock.fixed(t, java.time.ZoneOffset.UTC));
            assertEquals(1, s.stampCancelAll("a").sequenceNumber());
            assertEquals(2, s.stampCheck("0123456789abcdef").sequenceNumber());
            assertEquals(1, s.nextOrderId());
            assertThrows(IllegalArgumentException.class, () -> s.stampCancelAll("bad name"));
            assertEquals(3, s.nextSequenceNumber(), "an invalid command uses up no number");
        }

        @Test
        void skipCommandOnlySkipsEarlierCommands() {
            new SkipCommand(5, t, 4);
            assertThrows(IllegalArgumentException.class, () -> new SkipCommand(5, t, 5));
            assertThrows(IllegalArgumentException.class, () -> new SkipCommand(5, t, 6));
            assertThrows(IllegalArgumentException.class, () -> new SkipCommand(5, t, 0));
        }
    }
}
