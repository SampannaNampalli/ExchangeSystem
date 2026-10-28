package exchangeserver.book;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.Order;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;
import exchangeserver.engine.MatchingEngine;
import exchangeserver.engine.OrderOutput;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderBookTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    private final OrderBook book = new OrderBook();

    private static Order limit(long id, String participant, Side side, long price, long qty) {
        return new Order(id, OrderRequest.limit(participant, "c" + id, side, price, qty), T);
    }

    private static List<Long> prices(Collection<PriceLevel> levels) {
        List<Long> result = new ArrayList<>();
        for (PriceLevel l : levels) {
            result.add(l.getPrice());
        }
        return result;
    }

    @Nested
    class Add {

        @Test
        void bidsAreBestFirstHighestPrice() {
            book.add(limit(1, "a", Side.BUY, 99, 1));
            book.add(limit(2, "a", Side.BUY, 101, 1));
            book.add(limit(3, "a", Side.BUY, 100, 1));
            assertEquals(List.of(101L, 100L, 99L), prices(book.levels(Side.BUY)));
            assertEquals(101, book.bestLevel(Side.BUY).orElseThrow().getPrice());
        }

        @Test
        void asksAreBestFirstLowestPrice() {
            book.add(limit(1, "a", Side.SELL, 102, 1));
            book.add(limit(2, "a", Side.SELL, 100, 1));
            book.add(limit(3, "a", Side.SELL, 101, 1));
            assertEquals(List.of(100L, 101L, 102L), prices(book.levels(Side.SELL)));
        }

        @Test
        void sharesALevelInTimePriority() {
            Order a = limit(1, "a", Side.SELL, 100, 3);
            Order b = limit(2, "b", Side.SELL, 100, 4);
            book.add(a);
            book.add(b);
            assertEquals(1, book.levelCount(Side.SELL));
            assertEquals(7, book.volumeAt(Side.SELL, 100));
            assertEquals(List.of(a, b), book.ordersAt(Side.SELL, 100));
            book.checkInvariants();
        }

        @Test
        void duplicateIdThrowsAndChangesNothing() {
            book.add(limit(1, "a", Side.BUY, 100, 3));
            book.drainLevelUpdates();
            assertThrows(IllegalStateException.class, () -> book.add(limit(1, "a", Side.BUY, 99, 5)));
            assertEquals(1, book.levelCount(Side.BUY), "no empty level 99 left behind");
            assertEquals(1, book.openOrders("a"));
            assertTrue(book.drainLevelUpdates().isEmpty());
            book.checkInvariants();
        }

        @Test
        void refusesTypesThatNeverRest() {
            for (OrderRequest r : List.of(OrderRequest.ioc("a", "c", Side.BUY, 100, 1),
                    OrderRequest.fok("a", "c", Side.BUY, 100, 1), OrderRequest.market("a", "c", Side.BUY, 1))) {
                assertThrows(IllegalStateException.class, () -> book.add(new Order(1, r, T)), r.toString());
            }
            assertEquals(0, book.restingOrderCount());
        }

        @Test
        void refusesAnExhaustedOrder() {
            Order a = limit(1, "a", Side.BUY, 100, 3);
            a.fill(3);
            assertThrows(IllegalStateException.class, () -> book.add(a));
            assertEquals(0, book.levelCount(Side.BUY));
        }

        @Test
        void postOnlyRests() {
            book.add(new Order(1, OrderRequest.postOnly("a", "c", Side.BUY, 100, 2), T));
            assertTrue(book.isResting(1));
        }
    }

    @Nested
    class FillBest {

        @Test
        void partialFillLeavesTheOrderResting() {
            book.add(limit(1, "a", Side.SELL, 100, 10));
            Fill fill = book.fillBest(Side.SELL, 4);
            assertEquals(new Fill(1, "a", "c1", Side.SELL, 100, 4, 6), fill);
            assertTrue(book.isResting(1));
            assertEquals(6, book.volumeAt(Side.SELL, 100));
        }

        @Test
        void exhaustedOrderAndEmptiedLevelLeaveTheBook() {
            book.add(limit(1, "a", Side.SELL, 100, 3));
            book.add(limit(2, "b", Side.SELL, 101, 3));
            Fill fill = book.fillBest(Side.SELL, 10);
            assertEquals(3, fill.quantity());
            assertEquals(0, fill.leavesQuantity());
            assertFalse(book.isResting(1));
            assertEquals(List.of(101L), prices(book.levels(Side.SELL)));
            assertEquals(0, book.openOrders("a"));
            book.checkInvariants();
        }

        @Test
        void takesTheHeadOfTheBestLevel() {
            book.add(limit(1, "a", Side.BUY, 99, 5));
            book.add(limit(2, "b", Side.BUY, 100, 5));
            book.add(limit(3, "c", Side.BUY, 100, 5));
            assertEquals(2, book.fillBest(Side.BUY, 5).restingOrderId());
            assertEquals(3, book.fillBest(Side.BUY, 5).restingOrderId());
            assertEquals(1, book.fillBest(Side.BUY, 5).restingOrderId());
            assertEquals(0, book.restingOrderCount());
            book.checkInvariants();
        }

        @Test
        void emptySideThrows() {
            assertThrows(IllegalStateException.class, () -> book.fillBest(Side.SELL, 1));
        }
    }

    @Nested
    class Cancel {

        @Test
        void ownerCancelsTheRemainder() {
            book.add(limit(1, "a", Side.BUY, 100, 10));
            book.fillBest(Side.BUY, 4);
            assertEquals(CancelResult.cancelled(1, 6), book.cancel("a", 1));
            assertFalse(book.isResting(1));
            assertEquals(0, book.levelCount(Side.BUY));
            book.checkInvariants();
        }

        @Test
        void someoneElsesOrderLooksUnknown() {
            book.add(limit(1, "a", Side.BUY, 100, 10));
            assertEquals(CancelResult.unknown(1), book.cancel("b", 1));
            assertTrue(book.isResting(1));
        }

        @Test
        void unknownOrAlreadyGoneIsUnknown() {
            assertEquals(CancelStatus.UNKNOWN_ORDER, book.cancel("a", 42).status());
            book.add(limit(1, "a", Side.BUY, 100, 10));
            book.cancel("a", 1);
            assertEquals(CancelStatus.UNKNOWN_ORDER, book.cancel("a", 1).status());
        }

        @Test
        void cancelInTheMiddleKeepsTimePriority() {
            Order a = limit(1, "a", Side.SELL, 100, 1);
            Order b = limit(2, "a", Side.SELL, 100, 1);
            Order c = limit(3, "a", Side.SELL, 100, 1);
            book.add(a);
            book.add(b);
            book.add(c);
            book.cancel("a", 2);
            assertEquals(List.of(a, c), book.ordersAt(Side.SELL, 100));
            book.checkInvariants();
        }
    }

    @Nested
    class OpenOrders {

        @Test
        void countsPerParticipant() {
            book.add(limit(1, "a", Side.BUY, 100, 1));
            book.add(limit(2, "a", Side.SELL, 105, 1));
            book.add(limit(3, "b", Side.BUY, 99, 1));
            assertEquals(2, book.openOrders("a"));
            assertEquals(1, book.openOrders("b"));
            assertEquals(0, book.openOrders("nobody"));
            book.cancel("a", 1);
            book.fillBest(Side.SELL, 1);
            assertEquals(0, book.openOrders("a"));
            book.checkInvariants();
        }
    }

    @Nested
    class LevelUpdates {

        @Test
        void reportsTouchedLevelsInOrderOnce() {
            book.add(limit(1, "a", Side.SELL, 101, 5));
            book.add(limit(2, "a", Side.BUY, 99, 2));
            book.add(limit(3, "a", Side.SELL, 101, 1));
            assertEquals(List.of(new LevelUpdate(Side.SELL, 101, 6, 2), new LevelUpdate(Side.BUY, 99, 2, 1)),
                    book.drainLevelUpdates());
            assertTrue(book.drainLevelUpdates().isEmpty(), "drained");
        }

        @Test
        void removedLevelReportsZero() {
            book.add(limit(1, "a", Side.SELL, 101, 5));
            book.drainLevelUpdates();
            book.fillBest(Side.SELL, 5);
            assertEquals(List.of(new LevelUpdate(Side.SELL, 101, 0, 0)), book.drainLevelUpdates());
        }

        @Test
        void unknownCancelTouchesNothing() {
            book.cancel("a", 5);
            assertTrue(book.drainLevelUpdates().isEmpty());
        }
    }

    // Read access for the engine's fillability walk, cancel-all and snapshots (D67, D71, D72).
    @Nested
    class Walk {

        @Test
        void visitsInPriorityOrderAndStopsWhenAsked() {
            book.add(limit(1, "a", Side.SELL, 101, 1));
            book.add(limit(2, "b", Side.SELL, 100, 2));
            book.add(limit(3, "c", Side.SELL, 100, 3));
            List<Long> seen = new ArrayList<>();
            book.walk(Side.SELL, o -> {
                seen.add(o.orderId());
                return seen.size() < 2;
            });
            assertEquals(List.of(2L, 3L), seen, "best price first, time priority within it, then stop");
        }

        @Test
        void restingOrdersAreCopiesBidsFirst() {
            book.add(limit(1, "a", Side.SELL, 101, 5));
            book.add(limit(2, "b", Side.BUY, 99, 2));
            book.fillBest(Side.SELL, 3);
            List<RestingOrderView> all = book.restingOrders();
            assertEquals(List.of(2L, 1L), all.stream().map(RestingOrderView::orderId).toList());
            assertEquals(3, all.get(1).filledQuantity());
            assertEquals(2, all.get(1).remainingQuantity());
            book.checkInvariants();
        }

        @Test
        void headParticipantIsWhoTradesNext() {
            assertEquals(null, book.headParticipant(Side.SELL));
            book.add(limit(1, "a", Side.SELL, 101, 1));
            book.add(limit(2, "b", Side.SELL, 100, 1));
            assertEquals("b", book.headParticipant(Side.SELL));
        }
    }

    @Test
    void levelViewsAreReadOnly() {
        book.add(limit(1, "a", Side.BUY, 100, 1));
        assertThrows(UnsupportedOperationException.class, () -> book.levels(Side.BUY).clear());
        assertThrows(UnsupportedOperationException.class, () -> {
            var it = book.levels(Side.BUY).iterator();
            it.next();
            it.remove();
        });
    }

    // Random traffic through the real engine; every invariant is re-checked from scratch after every command.
    @Nested
    class InvariantsUnderRandomTraffic {

        @Test
        void tenThousandRandomCommands() {
            for (long seed = 1; seed <= 3; seed++) {
                runRandom(seed, 10_000);
            }
        }

        private void runRandom(long seed, int commands) {
            OrderBook b = new OrderBook();
            MatchingEngine engine = new MatchingEngine(b);
            Random random = new Random(seed);
            String[] participants = {"a", "b", "c", "d"};
            OrderType[] types = OrderType.values();
            long seq = 0;
            long id = 0;
            List<long[]> issued = new ArrayList<>();       // {orderId, participantIndex}

            for (int i = 0; i < commands; i++) {
                seq++;
                if (random.nextInt(4) == 0 && !issued.isEmpty()) {
                    long[] target = issued.get(random.nextInt(issued.size()));
                    // Sometimes the wrong participant: must be refused, never applied.
                    int who = random.nextInt(5) == 0 ? random.nextInt(participants.length) : (int) target[1];
                    engine.onCancel(new CancelOrderCommand(seq, T, participants[who], target[0]));
                } else {
                    id++;
                    int who = random.nextInt(participants.length);
                    OrderType type = types[random.nextInt(types.length)];
                    long qty = 1 + random.nextInt(20);
                    OptionalLong price = type == OrderType.MARKET ? OptionalLong.empty() : OptionalLong.of(95 + random.nextInt(11));
                    OrderRequest request = OrderRequest.of(participants[who], "c" + id, random.nextBoolean() ? Side.BUY : Side.SELL,
                            type, qty, price);
                    OrderOutput out = engine.onNewOrder(new NewOrderCommand(seq, T, id, request));
                    long traded = out.trades().stream().mapToLong(t -> t.quantity()).sum();
                    long passive = out.passiveFills().stream().mapToLong(f -> f.quantity()).sum();
                    assertEquals(traded, passive, "every trade has exactly one passive fill");
                    issued.add(new long[]{id, who});
                }
                b.checkInvariants();
            }
        }
    }
}
