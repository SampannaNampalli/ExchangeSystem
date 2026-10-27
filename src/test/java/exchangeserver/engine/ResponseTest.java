package exchangeserver.engine;

import exchangeserver.domain.Side;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The Response constructor is where contradictory fields fail (D22).
class ResponseTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");
    private static final Trade T5 = new Trade(100, 5, 1, 2, Side.BUY, T);

    @Test
    void consistentResponsesAreAccepted() {
        new Response(1, "c", OrderStatus.FILLED, null, 5, 0, List.of(T5));
        new Response(1, "c", OrderStatus.RESTED, null, 5, 3, List.of(T5));
        new Response(1, "c", OrderStatus.CANCELLED, null, 0, 3, List.of());
        new Response(1, "c", OrderStatus.REJECTED, RejectReason.POST_ONLY_WOULD_CROSS, 0, 3, List.of());
    }

    @Test
    void filledMustEqualTheSumOfTrades() {
        assertThrows(IllegalStateException.class, () -> new Response(1, "c", OrderStatus.FILLED, null, 4, 0, List.of(T5)));
    }

    @Test
    void filledMeansNothingRemains() {
        assertThrows(IllegalStateException.class, () -> new Response(1, "c", OrderStatus.FILLED, null, 5, 1, List.of(T5)));
    }

    @Test
    void restedAndCancelledMeanSomethingRemains() {
        assertThrows(IllegalStateException.class, () -> new Response(1, "c", OrderStatus.RESTED, null, 5, 0, List.of(T5)));
        assertThrows(IllegalStateException.class, () -> new Response(1, "c", OrderStatus.CANCELLED, null, 5, 0, List.of(T5)));
    }

    @Test
    void reasonGoesWithRejectedOnly() {
        assertThrows(IllegalStateException.class, () -> new Response(1, "c", OrderStatus.REJECTED, null, 0, 3, List.of()));
        assertThrows(IllegalStateException.class,
                () -> new Response(1, "c", OrderStatus.RESTED, RejectReason.MAX_OPEN_ORDERS, 0, 3, List.of()));
    }

    @Test
    void rejectedCannotHaveTraded() {
        assertThrows(IllegalStateException.class,
                () -> new Response(1, "c", OrderStatus.REJECTED, RejectReason.MAX_OPEN_ORDERS, 5, 0, List.of(T5)));
    }

    @Test
    void negativeRemainingIsRejected() {
        assertThrows(IllegalStateException.class, () -> new Response(1, "c", OrderStatus.CANCELLED, null, 0, -1, List.of()));
    }

    @Test
    void tradesAreCopied() {
        List<Trade> trades = new ArrayList<>(List.of(T5));
        Response r = new Response(1, "c", OrderStatus.FILLED, null, 5, 0, trades);
        trades.clear();
        assertEquals(1, r.trades().size());
        assertThrows(UnsupportedOperationException.class, () -> r.trades().clear());
    }

    @Test
    void tradeNeedsPositivePriceAndQuantity() {
        assertThrows(IllegalArgumentException.class, () -> new Trade(0, 5, 1, 2, Side.BUY, T));
        assertThrows(IllegalArgumentException.class, () -> new Trade(100, 0, 1, 2, Side.BUY, T));
    }
}
