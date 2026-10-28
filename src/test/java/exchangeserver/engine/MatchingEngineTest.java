package exchangeserver.engine;

import exchangeserver.book.CancelResult;
import exchangeserver.book.LevelUpdate;
import exchangeserver.book.OrderBook;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;
import exchangeserver.domain.Side;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static exchangeserver.domain.Side.BUY;
import static exchangeserver.domain.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    private final MatchingEngine engine = new MatchingEngine();
    private final OrderBook book = engine.book();
    private long seq;
    private long id;
    private int clientOrderIds;

    // ---------- helpers: stamp like the sequencer would ----------

    private OrderOutput submit(OrderRequest request) {
        seq++;
        id++;
        OrderOutput out = engine.onNewOrder(new NewOrderCommand(seq, T, id, request));
        book.drainLevelUpdates();       // no-op: the engine already drained; keeps tests independent of that
        return out;
    }

    private Response limit(String p, Side side, long price, long qty) {
        return submit(OrderRequest.limit(p, next(), side, price, qty)).response();
    }

    private Response ioc(String p, Side side, long price, long qty) {
        return submit(OrderRequest.ioc(p, next(), side, price, qty)).response();
    }

    private Response fok(String p, Side side, long price, long qty) {
        return submit(OrderRequest.fok(p, next(), side, price, qty)).response();
    }

    private Response market(String p, Side side, long qty) {
        return submit(OrderRequest.market(p, next(), side, qty)).response();
    }

    private Response postOnly(String p, Side side, long price, long qty) {
        return submit(OrderRequest.postOnly(p, next(), side, price, qty)).response();
    }

    private CancelOutput cancel(String p, long orderId) {
        seq++;
        return engine.onCancel(new CancelOrderCommand(seq, T, p, orderId));
    }

    private String next() {
        return "c" + (++clientOrderIds);
    }

    private static Trade trade(long price, long qty, long buyId, long sellId, Side aggressor) {
        return new Trade(price, qty, buyId, sellId, aggressor, T);
    }

    // ---------- tests ----------

    @Nested
    class Limit {

        @Test
        void restsOnAnEmptyBook() {
            Response r = limit("a", BUY, 100, 10);
            assertEquals(OrderStatus.RESTED, r.status());
            assertEquals(10, r.remainingQuantity());
            assertTrue(r.trades().isEmpty());
            assertEquals(10, book.volumeAt(BUY, 100));
        }

        @Test
        void tradesAtTheRestingPrice() {
            limit("s", SELL, 100, 5);                       // id 1
            Response r = limit("b", BUY, 105, 5);           // id 2, willing to pay 105
            assertEquals(OrderStatus.FILLED, r.status());
            assertEquals(List.of(trade(100, 5, 2, 1, BUY)), r.trades());
            assertEquals(0, book.restingOrderCount());
        }

        @Test
        void partialFillRestsTheRemainder() {
            limit("s", SELL, 100, 5);
            Response r = limit("b", BUY, 100, 8);
            assertEquals(OrderStatus.RESTED, r.status());
            assertEquals(5, r.filledQuantity());
            assertEquals(3, r.remainingQuantity());
            assertEquals(3, book.volumeAt(BUY, 100));
            assertEquals(0, book.levelCount(SELL));
        }

        @Test
        void sweepsLevelsBestPriceFirstAndStopsAtItsLimit() {
            limit("s", SELL, 102, 4);       // 1
            limit("s", SELL, 100, 2);       // 2
            limit("s", SELL, 101, 3);       // 3
            Response r = limit("b", BUY, 101, 10);         // 4
            assertEquals(List.of(trade(100, 2, 4, 2, BUY), trade(101, 3, 4, 3, BUY)), r.trades());
            assertEquals(OrderStatus.RESTED, r.status());
            assertEquals(5, book.volumeAt(BUY, 101));
            assertEquals(4, book.volumeAt(SELL, 102), "102 is above the limit");
        }

        @Test
        void timePriorityWithinALevel() {
            limit("s1", SELL, 100, 2);      // 1, first in
            limit("s2", SELL, 100, 2);      // 2
            Response r = limit("b", BUY, 100, 3);
            assertEquals(List.of(trade(100, 2, 3, 1, BUY), trade(100, 1, 3, 2, BUY)), r.trades());
            assertEquals(1, book.volumeAt(SELL, 100));
            assertTrue(book.isResting(2));
            assertFalse(book.isResting(1));
        }

        @Test
        void sellSideIsTheMirrorImage() {
            limit("b", BUY, 99, 3);         // 1
            limit("b", BUY, 100, 3);        // 2
            Response r = limit("s", SELL, 99, 5);           // 3
            assertEquals(List.of(trade(100, 3, 2, 3, SELL), trade(99, 2, 1, 3, SELL)), r.trades());
            assertEquals(OrderStatus.FILLED, r.status());
            assertEquals(1, book.volumeAt(BUY, 99));
        }

        @Test
        void allSixPriceRelationsBetweenSidesAndPrices() {
            // incoming side x {below, equal, above} the resting price: crosses iff the buyer pays >= the ask.
            long[][] cases = {
                    // incoming BUY vs resting ask 100
                    {99, 0}, {100, 1}, {101, 1},
            };
            for (long[] c : cases) {
                MatchingEngineTest t = new MatchingEngineTest();
                t.limit("s", SELL, 100, 1);
                assertEquals(c[1] == 1, !t.limit("b", BUY, c[0], 1).trades().isEmpty(), "buy at " + c[0]);
            }
            long[][] sells = {
                    // incoming SELL vs resting bid 100
                    {101, 0}, {100, 1}, {99, 1},
            };
            for (long[] c : sells) {
                MatchingEngineTest t = new MatchingEngineTest();
                t.limit("b", BUY, 100, 1);
                assertEquals(c[1] == 1, !t.limit("s", SELL, c[0], 1).trades().isEmpty(), "sell at " + c[0]);
            }
        }

        @Test
        void nonCrossingOrdersBothRest() {
            limit("b", BUY, 99, 1);
            limit("s", SELL, 100, 1);
            assertEquals(1, book.levelCount(BUY));
            assertEquals(1, book.levelCount(SELL));
        }
    }

    @Nested
    class Ioc {

        @Test
        void fullFillIsFilled() {
            limit("s", SELL, 100, 5);
            Response r = ioc("b", BUY, 100, 5);
            assertEquals(OrderStatus.FILLED, r.status());
            assertEquals(0, r.remainingQuantity());
        }

        @Test
        void partialFillCancelsTheRemainderInsteadOfResting() {
            limit("s", SELL, 100, 5);
            Response r = ioc("b", BUY, 100, 8);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(5, r.filledQuantity());
            assertEquals(3, r.remainingQuantity());
            assertEquals(0, book.restingOrderCount());
        }

        @Test
        void nothingCrossesCancelsAll() {
            limit("s", SELL, 101, 5);
            Response r = ioc("b", BUY, 100, 8);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(8, r.remainingQuantity());
            assertEquals(1, book.restingOrderCount());
        }
    }

    @Nested
    class Fok {

        @Test
        void fillsAcrossLevels() {
            limit("s", SELL, 100, 2);
            limit("s", SELL, 101, 3);
            Response r = fok("b", BUY, 101, 5);
            assertEquals(OrderStatus.FILLED, r.status());
            assertEquals(2, r.trades().size());
            assertEquals(0, book.restingOrderCount());
        }

        @Test
        void exactlyEnoughVolumeFills() {
            limit("s", SELL, 100, 2);
            limit("s", SELL, 100, 3);
            assertEquals(OrderStatus.FILLED, fok("b", BUY, 100, 5).status());
        }

        @Test
        void notEnoughVolumeKillsItWithNoTradesAndNoChange() {
            limit("s", SELL, 100, 2);
            limit("s", SELL, 101, 3);
            Response r = fok("b", BUY, 101, 6);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(6, r.remainingQuantity());
            assertTrue(r.trades().isEmpty());
            assertEquals(2, book.volumeAt(SELL, 100));
            assertEquals(3, book.volumeAt(SELL, 101));
        }

        @Test
        void volumeBeyondItsPriceDoesNotCount() {
            limit("s", SELL, 100, 2);
            limit("s", SELL, 102, 10);
            assertEquals(OrderStatus.CANCELLED, fok("b", BUY, 101, 5).status());
            assertEquals(2, book.volumeAt(SELL, 100));
        }

        @Test
        void sellFokAgainstBids() {
            limit("b", BUY, 100, 4);
            limit("b", BUY, 99, 4);
            assertEquals(OrderStatus.FILLED, fok("s", SELL, 99, 8).status());
            assertEquals(0, book.restingOrderCount());
        }

        @Test
        void emptyBookKills() {
            assertEquals(OrderStatus.CANCELLED, fok("b", BUY, 100, 1).status());
        }
    }

    @Nested
    class Market {

        // D66: before the first trade the collar is anchored on the touch, so a market order cannot sweep a thin
        // book (it used to take 100 and then 500).
        @Test
        void beforeTheFirstTradeTheCollarIsAnchoredOnTheTouch() {
            limit("s", SELL, 100, 1);
            limit("s", SELL, 110, 1);       // touch + 10%: inside
            limit("s", SELL, 500, 1);
            Response r = market("b", BUY, 3);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(List.of(100L, 110L), r.trades().stream().map(Trade::price).toList());
            assertEquals(1, book.volumeAt(SELL, 500), "far outside the collar: untouched");
        }

        @Test
        void beforeTheFirstTradeASellCollarRoundsInwardFromTheTouch() {
            limit("b", BUY, 101, 1);        // collar for sells: 90.9 -> 91
            limit("b", BUY, 91, 1);
            limit("b", BUY, 90, 1);
            Response r = market("s", SELL, 3);
            assertEquals(List.of(101L, 91L), r.trades().stream().map(Trade::price).toList());
            assertEquals(1, book.volumeAt(BUY, 90));
        }

        @Test
        void emptyBookCancelsAll() {
            Response r = market("b", BUY, 5);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(5, r.remainingQuantity());
        }

        @Test
        void otherSideRunsOutCancelsTheRemainder() {
            limit("s", SELL, 100, 3);
            Response r = market("b", BUY, 5);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(3, r.filledQuantity());
            assertEquals(2, r.remainingQuantity());
            assertEquals(0, book.restingOrderCount(), "a market order never rests");
        }

        @Test
        void buyCollarStopsAtReferencePlusBand() {
            limit("s", SELL, 111, 1);       // placed before any trade, so no band applied to it
            establishLastTrade(100);        // collar for buys: 110
            limit("s", SELL, 105, 1);
            limit("s", SELL, 110, 1);       // exactly 100 + 10%: inside
            Response r = market("b", BUY, 5);
            assertEquals(List.of(105L, 110L), r.trades().stream().map(Trade::price).toList());
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(1, book.volumeAt(SELL, 111), "outside the collar: untouched");
        }

        @Test
        void sellCollarRoundsInward() {
            limit("b", BUY, 90, 1);         // before any trade
            establishLastTrade(101);        // collar for sells: 90.9 -> 91
            limit("b", BUY, 91, 1);
            limit("b", BUY, 92, 1);
            Response r = market("s", SELL, 5);
            assertEquals(List.of(92L, 91L), r.trades().stream().map(Trade::price).toList());
            assertEquals(1, book.volumeAt(BUY, 90));
        }

        @Test
        void collarIsFixedAtArrivalNotMovedByItsOwnTrades() {
            limit("s", SELL, 115, 1);       // before any trade
            establishLastTrade(100);        // collar 110
            limit("s", SELL, 108, 1);
            // Trading at 108 moves the reference to 108 (a moving collar would reach 118 and take 115);
            // this order's collar stays at 110.
            Response r = market("b", BUY, 3);
            assertEquals(1, r.filledQuantity());
            assertEquals(1, book.volumeAt(SELL, 115));
            assertEquals(108, engine.lastTradePrice());
        }

        private void establishLastTrade(long price) {
            limit("x", SELL, price, 1);
            limit("y", BUY, price, 1);
            assertEquals(price, engine.lastTradePrice());
        }
    }

    @Nested
    class PostOnly {

        @Test
        void restsWhenItDoesNotCross() {
            limit("s", SELL, 101, 1);
            Response r = postOnly("b", BUY, 100, 4);
            assertEquals(OrderStatus.RESTED, r.status());
            assertEquals(4, book.volumeAt(BUY, 100));
        }

        @Test
        void equalPriceCountsAsCrossingAndIsRejected() {
            limit("s", SELL, 100, 1);
            Response r = postOnly("b", BUY, 100, 4);
            assertEquals(OrderStatus.REJECTED, r.status());
            assertEquals(Reason.POST_ONLY_WOULD_CROSS, r.reason());
            assertEquals(4, r.remainingQuantity());
            assertEquals(0, book.levelCount(BUY), "book unchanged");
            assertEquals(1, book.volumeAt(SELL, 100));
        }

        @Test
        void sellThatWouldCrossIsRejected() {
            limit("b", BUY, 100, 1);
            assertEquals(OrderStatus.REJECTED, postOnly("s", SELL, 99, 1).status());
        }

        @Test
        void restsOnAnEmptyBook() {
            assertEquals(OrderStatus.RESTED, postOnly("s", SELL, 99, 1).status());
        }
    }

    @Nested
    class Cancel {

        @Test
        void ownerCancelsARestingOrder() {
            limit("a", BUY, 100, 10);       // id 1
            CancelOutput out = cancel("a", 1);
            assertEquals(CancelResult.cancelled(1, 10), out.result());
            assertEquals(List.of(new LevelUpdate(BUY, 100, 0, 0)), out.levelUpdates());
            assertEquals(0, book.restingOrderCount());
        }

        @Test
        void cancelsWhatIsLeftAfterAPartialFill() {
            limit("a", BUY, 100, 10);       // 1
            limit("s", SELL, 100, 4);       // 2
            assertEquals(6, cancel("a", 1).result().cancelledQuantity());
        }

        @Test
        void filledOrderIsUnknown() {
            limit("a", BUY, 100, 4);
            limit("s", SELL, 100, 4);
            assertEquals(CancelResult.unknown(1), cancel("a", 1).result());
        }

        @Test
        void anotherParticipantCannotCancel() {
            limit("a", BUY, 100, 4);
            CancelOutput out = cancel("mallory", 1);
            assertEquals(CancelResult.unknown(1), out.result());
            assertTrue(out.levelUpdates().isEmpty());
            assertTrue(book.isResting(1));
        }

        @Test
        void neverIssuedIdIsUnknown() {
            assertEquals(CancelResult.unknown(99), cancel("a", 99).result());
        }
    }

    @Nested
    class Risk {

        @Test
        void duplicateClientOrderIdIsRejectedWithTheOriginalOrderId() {
            submit(OrderRequest.limit("a", "X", BUY, 100, 1));                       // id 1
            Response r = submit(OrderRequest.limit("a", "X", BUY, 99, 5)).response();    // id 2
            assertEquals(OrderStatus.REJECTED, r.status());
            assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, r.reason());
            assertEquals(1, r.orderId());
            assertEquals(1, book.volumeAt(BUY, 100), "the original still rests");
            assertEquals(0, book.volumeAt(BUY, 99), "the duplicate did not");
        }

        @Test
        void sameClientOrderIdForDifferentParticipantsIsFine() {
            submit(OrderRequest.limit("a", "X", BUY, 100, 1));
            assertEquals(OrderStatus.RESTED, submit(OrderRequest.limit("b", "X", BUY, 100, 1)).response().status());
        }

        @Test
        void aRejectedOrderStillUsesUpItsClientOrderId() {
            limit("s", SELL, 100, 1);
            submit(OrderRequest.postOnly("a", "X", BUY, 100, 1));       // rejected: would cross
            Response again = submit(OrderRequest.postOnly("a", "X", BUY, 99, 1)).response();
            assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, again.reason());
        }

        @Test
        void noBandBeforeTheFirstTrade() {
            assertEquals(OrderStatus.RESTED, limit("a", BUY, 1, 1).status());
            assertEquals(OrderStatus.RESTED, limit("a", SELL, 1_000_000, 1).status());
        }

        @Test
        void bandIsInclusiveAroundTheLastTrade() {
            limit("x", SELL, 100, 1);
            limit("y", BUY, 100, 1);        // last trade 100: band 90..110
            assertEquals(OrderStatus.RESTED, limit("a", BUY, 90, 1).status());
            assertEquals(OrderStatus.RESTED, limit("a", SELL, 110, 1).status());
            Response low = limit("a", BUY, 89, 1);
            Response high = limit("a", SELL, 111, 1);
            assertEquals(Reason.PRICE_OUTSIDE_BAND, low.reason());
            assertEquals(Reason.PRICE_OUTSIDE_BAND, high.reason());
            assertEquals(0, book.volumeAt(BUY, 89));
        }

        @Test
        void bandAppliesToEveryPricedType() {
            limit("x", SELL, 100, 1);
            limit("y", BUY, 100, 1);
            assertEquals(Reason.PRICE_OUTSIDE_BAND, ioc("a", BUY, 200, 1).reason());
            assertEquals(Reason.PRICE_OUTSIDE_BAND, fok("a", BUY, 200, 1).reason());
            assertEquals(Reason.PRICE_OUTSIDE_BAND, postOnly("a", BUY, 50, 1).reason());
        }

        @Test
        void openOrderCapRejectsRestingTypesButNotImmediateOnes() {
            for (int i = 0; i < Rules.MAX_OPEN_ORDERS_PER_PARTICIPANT; i++) {
                limit("a", BUY, 100, 1);
            }
            assertEquals(Reason.MAX_OPEN_ORDERS, limit("a", BUY, 100, 1).reason());
            assertEquals(Reason.MAX_OPEN_ORDERS, postOnly("a", BUY, 100, 1).reason());
            assertEquals(OrderStatus.CANCELLED, ioc("a", SELL, 101, 1).status(), "IOC never rests: allowed");
            assertEquals(OrderStatus.RESTED, limit("b", BUY, 100, 1).status(), "the cap is per participant");
            cancel("a", 1);
            assertEquals(OrderStatus.RESTED, limit("a", BUY, 100, 1).status(), "room again after a cancel");
        }
    }

    // D67: an incoming order never trades with its own participant's resting order; its remainder is cancelled.
    @Nested
    class SelfTrade {

        @Test
        void aLimitThatWouldCrossItsOwnOrderIsCancelledNotRested() {
            limit("a", SELL, 100, 5);
            Response r = limit("a", BUY, 100, 5);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(Reason.SELF_TRADE_PREVENTED, r.reason());
            assertEquals(5, r.remainingQuantity());
            assertTrue(r.trades().isEmpty());
            assertEquals(5, book.volumeAt(SELL, 100), "the resting order is untouched");
            assertEquals(0, book.volumeAt(BUY, 100), "and the incoming one did not rest crossed");
        }

        @Test
        void tradesWithOthersAheadThenStopsAtItsOwnOrder() {
            limit("x", SELL, 100, 2);
            limit("a", SELL, 100, 3);
            limit("y", SELL, 100, 4);       // behind a's order: not reached
            Response r = ioc("a", BUY, 100, 9);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(Reason.SELF_TRADE_PREVENTED, r.reason());
            assertEquals(2, r.filledQuantity());
            assertEquals(7, book.volumeAt(SELL, 100));
        }

        @Test
        void itStopsRatherThanSkippingToWorsePrices() {
            limit("a", SELL, 100, 1);
            limit("x", SELL, 101, 1);
            Response r = limit("a", BUY, 101, 1);
            assertEquals(Reason.SELF_TRADE_PREVENTED, r.reason());
            assertEquals(1, book.volumeAt(SELL, 101), "price priority holds: the own order at 100 comes first");
        }

        @Test
        void aMarketOrderIsCancelledWithTheReason() {
            limit("a", SELL, 100, 3);
            Response r = market("a", BUY, 3);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(Reason.SELF_TRADE_PREVENTED, r.reason());
        }

        @Test
        void aFokThatOnlyItsOwnOrderWouldCompleteIsKilledWithTheReason() {
            limit("x", SELL, 100, 2);
            limit("a", SELL, 100, 3);
            Response blocked = fok("a", BUY, 100, 5);
            assertEquals(OrderStatus.CANCELLED, blocked.status());
            assertEquals(Reason.SELF_TRADE_PREVENTED, blocked.reason());
            assertEquals(5, book.volumeAt(SELL, 100), "a FOK that does not go ahead changes nothing");
            assertEquals(OrderStatus.FILLED, fok("a", BUY, 100, 2).status(), "what others offer ahead is enough");
        }

        // The pre-check must stop at the own order exactly as the match loop does: volume behind it does not count.
        @Test
        void volumeBehindItsOwnOrderDoesNotMakeAFokFillable() {
            limit("x", SELL, 100, 2);
            limit("a", SELL, 100, 3);
            limit("y", SELL, 100, 5);
            Response r = fok("a", BUY, 100, 5);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertEquals(Reason.SELF_TRADE_PREVENTED, r.reason());
            assertEquals(10, book.volumeAt(SELL, 100));
        }

        @Test
        void aFokShortOfVolumeWithoutItsOwnOrderHasNoReason() {
            limit("x", SELL, 100, 2);
            Response r = fok("a", BUY, 100, 5);
            assertEquals(OrderStatus.CANCELLED, r.status());
            assertNull(r.reason());
        }

        @Test
        void otherParticipantsTradeAsBefore() {
            limit("a", SELL, 100, 5);
            assertEquals(OrderStatus.FILLED, limit("b", BUY, 100, 5).status());
        }
    }

    // D68: the caps bound what rests; an order that fully trades on arrival is never refused for them.
    @Nested
    class Caps {

        private final MatchingEngine small = new MatchingEngine(new OrderBook(), new MatchingEngine.Limits(2, 3));
        private long s;

        private Response send(OrderRequest request) {
            s++;
            return small.onNewOrder(new NewOrderCommand(s, T, s, request)).response();
        }

        @Test
        void atTheParticipantCapAFullyFillableLimitIsAccepted() {
            send(OrderRequest.limit("x", "x1", SELL, 101, 5));
            send(OrderRequest.limit("a", "a1", BUY, 99, 1));
            send(OrderRequest.limit("a", "a2", BUY, 99, 1));             // a is at its cap of 2
            assertEquals(OrderStatus.FILLED, send(OrderRequest.limit("a", "a3", BUY, 101, 5)).status());
        }

        @Test
        void atTheParticipantCapARemainderThatWouldRestIsRejected() {
            send(OrderRequest.limit("x", "x1", SELL, 101, 2));
            send(OrderRequest.limit("a", "a1", BUY, 99, 1));
            send(OrderRequest.limit("a", "a2", BUY, 99, 1));
            Response r = send(OrderRequest.limit("a", "a3", BUY, 101, 5));
            assertEquals(Reason.MAX_OPEN_ORDERS, r.reason());
            assertEquals(2, small.snapshot(1).asks().get(0).volume(), "rejected before any trade");
        }

        @Test
        void aPostOnlyAtTheCapIsRejected() {
            send(OrderRequest.limit("a", "a1", BUY, 99, 1));
            send(OrderRequest.limit("a", "a2", BUY, 99, 1));
            assertEquals(Reason.MAX_OPEN_ORDERS, send(OrderRequest.postOnly("a", "a3", BUY, 98, 1)).reason());
        }

        @Test
        void aFullBookRejectsWhatWouldRestButNotWhatTrades() {
            send(OrderRequest.limit("a", "a1", BUY, 99, 1));
            send(OrderRequest.limit("b", "b1", BUY, 98, 1));
            send(OrderRequest.limit("c", "c1", SELL, 101, 1));          // 3 resting: the book is full
            assertEquals(Reason.BOOK_FULL, send(OrderRequest.limit("d", "d1", BUY, 97, 1)).reason());
            assertEquals(OrderStatus.FILLED, send(OrderRequest.limit("d", "d2", BUY, 101, 1)).status());
            assertEquals(OrderStatus.RESTED, send(OrderRequest.limit("d", "d3", BUY, 97, 1)).status(), "room again");
        }
    }

    // D72: CANCEL_ALL removes every resting order of one participant, nobody else's.
    @Nested
    class CancelAll {

        @Test
        void cancelsOnlyThatParticipantsOrdersInBookPriority() {
            limit("a", BUY, 99, 1);         // id 1
            limit("b", BUY, 99, 2);         // id 2
            limit("a", SELL, 101, 3);       // id 3
            limit("a", BUY, 100, 4);        // id 4: the best bid, so first
            seq++;
            CancelAllOutput out = engine.onCancelAll(new exchangeserver.command.CancelAllCommand(seq, T, "a"));
            assertEquals(List.of(CancelResult.cancelled(4, 4), CancelResult.cancelled(1, 1), CancelResult.cancelled(3, 3)),
                    out.results());
            assertEquals(1, book.restingOrderCount());
            assertEquals(2, book.volumeAt(BUY, 99), "b's order stays");
            assertEquals(List.of(new LevelUpdate(BUY, 100, 0, 0), new LevelUpdate(BUY, 99, 2, 1), new LevelUpdate(SELL, 101, 0, 0)),
                    out.levelUpdates());
        }

        @Test
        void aParticipantWithNothingRestingGetsNoResults() {
            seq++;
            assertTrue(engine.onCancelAll(new exchangeserver.command.CancelAllCommand(seq, T, "nobody")).results().isEmpty());
        }

        @Test
        void itsSequenceNumberMustIncreaseLikeAnyCommand() {
            limit("a", BUY, 99, 1);
            assertThrows(IllegalStateException.class,
                    () -> engine.onCancelAll(new exchangeserver.command.CancelAllCommand(seq, T, "a")));
            assertEquals(1, book.restingOrderCount(), "refused before any change");
        }
    }

    // D71: a snapshot holds enough state that the engine continues exactly as if it had replayed everything.
    @Nested
    class SnapshotState {

        @Test
        void exportThenRestoreBehavesIdentically() {
            limit("a", SELL, 100, 5);
            limit("b", SELL, 100, 3);       // behind a: time priority must survive
            limit("x", BUY, 100, 2);        // trade: a's order partly filled, last trade 100
            limit("c", BUY, 95, 4);
            MatchingEngine restored = MatchingEngine.restore(engine.exportState());
            assertEquals(engine.snapshot(10), restored.snapshot(10));

            NewOrderCommand next = new NewOrderCommand(seq + 1, T, id + 1, OrderRequest.limit("d", "d1", BUY, 100, 4));
            assertEquals(engine.onNewOrder(next), restored.onNewOrder(next), "same fills in the same order");
            NewOrderCommand far = new NewOrderCommand(seq + 2, T, id + 2, OrderRequest.limit("d", "d2", BUY, 50, 1));
            assertEquals(Reason.PRICE_OUTSIDE_BAND, restored.onNewOrder(far).response().reason(), "the band reference survived");
        }

        @Test
        void restingClientOrderIdsAreStillDuplicatesAfterARestore() {
            submit(OrderRequest.limit("a", "keep", BUY, 90, 1));
            MatchingEngine restored = MatchingEngine.restore(engine.exportState());
            NewOrderCommand again = new NewOrderCommand(seq + 1, T, id + 1, OrderRequest.limit("a", "keep", BUY, 91, 1));
            assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, restored.onNewOrder(again).response().reason());
        }

        @Test
        void counterViolationsStillFailAfterARestore() {
            limit("a", BUY, 90, 1);
            MatchingEngine restored = MatchingEngine.restore(engine.exportState());
            assertThrows(IllegalStateException.class,
                    () -> restored.onNewOrder(new NewOrderCommand(seq + 1, T, id, OrderRequest.limit("a", "z", BUY, 90, 1))));
        }
    }

    @Nested
    class Outputs {

        @Test
        void passiveFillsGoToTheRestingOwnersWithLeaves() {
            submit(OrderRequest.limit("alice", "a1", SELL, 100, 5));      // 1
            submit(OrderRequest.limit("bob", "b1", SELL, 101, 5));        // 2
            OrderOutput out = submit(OrderRequest.limit("carol", "c1", BUY, 101, 7));
            assertEquals(List.of(
                    new Execution("alice", 1, "a1", SELL, 100, 5, 0),
                    new Execution("bob", 2, "b1", SELL, 101, 2, 3)), out.passiveFills());
        }

        @Test
        void levelUpdatesCoverEveryTouchedLevelInOrder() {
            limit("s", SELL, 100, 5);
            limit("s", SELL, 101, 5);
            OrderOutput out = submit(OrderRequest.limit("b", "z", BUY, 101, 7));
            assertEquals(List.of(
                    new LevelUpdate(SELL, 100, 0, 0),
                    new LevelUpdate(SELL, 101, 3, 1)), out.levelUpdates());
        }

        @Test
        void restingRemainderAppearsAsABidLevel() {
            limit("s", SELL, 100, 2);
            OrderOutput out = submit(OrderRequest.limit("b", "z", BUY, 100, 5));
            assertEquals(List.of(new LevelUpdate(SELL, 100, 0, 0), new LevelUpdate(BUY, 100, 3, 1)), out.levelUpdates());
        }

        @Test
        void rejectedOrderChangesNoLevel() {
            limit("s", SELL, 100, 2);
            assertTrue(submit(OrderRequest.postOnly("b", "z", BUY, 100, 5)).levelUpdates().isEmpty());
        }

        @Test
        void outputCarriesTheCommandsSequenceAndTimestamp() {
            OrderOutput out = submit(OrderRequest.limit("b", "z", BUY, 100, 5));
            assertEquals(seq, out.sequenceNumber());
            assertEquals(T, out.timestamp());
        }

        @Test
        void tradeTimestampIsTheIncomingCommandsTimestamp() {
            limit("s", SELL, 100, 1);
            Instant later = T.plusSeconds(60);
            seq++;
            id++;
            OrderOutput out = engine.onNewOrder(new NewOrderCommand(seq, later, id, OrderRequest.limit("b", "z", BUY, 100, 1)));
            assertEquals(later, out.trades().get(0).timestamp());
        }

        @Test
        void quantityIsConserved() {
            limit("s", SELL, 100, 3);
            limit("s", SELL, 101, 3);
            Response r = limit("b", BUY, 101, 10);
            long traded = r.trades().stream().mapToLong(Trade::quantity).sum();
            assertEquals(10, traded + r.remainingQuantity());
            assertEquals(traded, r.filledQuantity());
        }
    }

    @Nested
    class Snapshot {

        @Test
        void bestFirstWithDepthLastTradeAndSequence() {
            limit("s", SELL, 103, 1);
            limit("s", SELL, 101, 2);
            limit("s", SELL, 102, 3);
            limit("b", BUY, 99, 4);
            limit("b", BUY, 98, 5);
            BookSnapshot s = engine.snapshot(2);
            assertEquals(5, s.sequenceNumber());
            assertEquals(5, engine.lastSequenceNumber());
            assertEquals(0, s.lastTradePrice());
            assertEquals(List.of(new LevelUpdate(BUY, 99, 4, 1), new LevelUpdate(BUY, 98, 5, 1)), s.bids());
            assertEquals(List.of(new LevelUpdate(SELL, 101, 2, 1), new LevelUpdate(SELL, 102, 3, 1)), s.asks());
        }

        @Test
        void depthMustBePositive() {
            assertThrows(IllegalArgumentException.class, () -> engine.snapshot(0));
        }
    }

    @Nested
    class Guards {

        @Test
        void orderIdMustIncreaseAndAViolationChangesNothing() {
            engine.onNewOrder(new NewOrderCommand(1, T, 5, OrderRequest.limit("a", "c1", BUY, 100, 1)));
            assertThrows(IllegalStateException.class,
                    () -> engine.onNewOrder(new NewOrderCommand(2, T, 5, OrderRequest.limit("a", "c2", BUY, 99, 1))));
            assertThrows(IllegalStateException.class,
                    () -> engine.onNewOrder(new NewOrderCommand(2, T, 4, OrderRequest.limit("a", "c2", BUY, 99, 1))));
            assertEquals(1, book.restingOrderCount());
            // Gaps are fine (D16).
            engine.onNewOrder(new NewOrderCommand(2, T, 9, OrderRequest.limit("a", "c3", BUY, 99, 1)));
        }

        @Test
        void sequenceNumberMustIncrease() {
            engine.onNewOrder(new NewOrderCommand(3, T, 1, OrderRequest.limit("a", "c1", BUY, 100, 1)));
            assertThrows(IllegalStateException.class, () -> engine.onCancel(new CancelOrderCommand(3, T, "a", 1)));
            assertThrows(IllegalStateException.class,
                    () -> engine.onNewOrder(new NewOrderCommand(2, T, 2, OrderRequest.limit("a", "c2", BUY, 100, 1))));
            assertTrue(book.isResting(1));
        }

        @Test
        void bookPassedInMustBeEmpty() {
            OrderBook used = new OrderBook();
            new MatchingEngine(used).onNewOrder(new NewOrderCommand(1, T, 1, OrderRequest.limit("a", "c", BUY, 100, 1)));
            assertThrows(IllegalArgumentException.class, () -> new MatchingEngine(used));
        }
    }

    @Nested
    class Determinism {

        @Test
        void sameCommandsGiveIdenticalOutputs() {
            List<Command> commands = new ArrayList<>();
            java.util.Random random = new java.util.Random(42);
            for (int i = 1; i <= 2_000; i++) {
                if (i % 5 == 0) {
                    commands.add(new CancelOrderCommand(i, T.plusMillis(i), "p" + random.nextInt(3), 1 + random.nextInt(i)));
                } else {
                    Side side = random.nextBoolean() ? BUY : SELL;
                    commands.add(new NewOrderCommand(i, T.plusMillis(i), i,
                            OrderRequest.limit("p" + random.nextInt(3), "c" + i, side, 95 + random.nextInt(11), 1 + random.nextInt(9))));
                }
            }
            assertEquals(run(commands), run(commands));
        }

        private List<EngineOutput> run(List<Command> commands) {
            MatchingEngine e = new MatchingEngine();
            List<EngineOutput> outputs = new ArrayList<>();
            for (Command c : commands) {
                outputs.add(c instanceof NewOrderCommand n ? e.onNewOrder(n) : e.onCancel((CancelOrderCommand) c));
            }
            return outputs;
        }
    }

    @Test
    void crossesIsInclusiveOnBothSides() {
        assertTrue(MatchingEngine.crosses(BUY, 100, 100));
        assertTrue(MatchingEngine.crosses(BUY, 100, 99));
        assertFalse(MatchingEngine.crosses(BUY, 100, 101));
        assertTrue(MatchingEngine.crosses(SELL, 100, 100));
        assertTrue(MatchingEngine.crosses(SELL, 100, 101));
        assertFalse(MatchingEngine.crosses(SELL, 100, 99));
    }

    @Test
    void onlyRejectedResponsesCarryAReason() {
        limit("s", SELL, 100, 2);
        assertNull(limit("b", BUY, 90, 5).reason());
        Response r = postOnly("b", BUY, 100, 5);
        assertEquals(Reason.POST_ONLY_WOULD_CROSS, r.reason());
        assertEquals(0, r.filledQuantity());
        assertTrue(r.trades().isEmpty());
    }
}
