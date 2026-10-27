package exchangeserver.domain;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    @Nested
    class Construction {

        @Test
        void copiesTheRequestAndStamps() {
            Order o = new Order(7, OrderRequest.limit("alice", "c1", Side.SELL, 101, 25), T);
            assertEquals(7, o.getOrderId());
            assertEquals("alice", o.getParticipant());
            assertEquals("c1", o.getClientOrderId());
            assertEquals(Side.SELL, o.getSide());
            assertEquals(OrderType.LIMIT, o.getOrderType());
            assertEquals(101, o.getPrice());
            assertEquals(25, o.getOriginalQuantity());
            assertEquals(25, o.getRemainingQuantity());
            assertEquals(0, o.getFilledQuantity());
            assertEquals(T, o.getTimestamp());
        }

        @Test
        void idMustBePositive() {
            OrderRequest r = OrderRequest.limit("a", "c", Side.BUY, 100, 1);
            assertThrows(IllegalArgumentException.class, () -> new Order(0, r, T));
            assertThrows(IllegalArgumentException.class, () -> new Order(-5, r, T));
        }

        @Test
        void requestAndTimestampAreRequired() {
            assertThrows(NullPointerException.class, () -> new Order(1, null, T));
            assertThrows(NullPointerException.class, () -> new Order(1, OrderRequest.limit("a", "c", Side.BUY, 100, 1), null));
        }
    }

    @Nested
    class Price {

        @Test
        void marketOrderHasNoPriceAndSaysSo() {
            Order o = new Order(1, OrderRequest.market("a", "c", Side.BUY, 5), T);
            assertFalse(o.hasPrice());
            assertThrows(IllegalStateException.class, o::getPrice);
        }

        @Test
        void everyOtherTypeHasAPrice() {
            assertTrue(new Order(1, OrderRequest.ioc("a", "c", Side.BUY, 9, 5), T).hasPrice());
            assertTrue(new Order(1, OrderRequest.fok("a", "c", Side.BUY, 9, 5), T).hasPrice());
            assertTrue(new Order(1, OrderRequest.postOnly("a", "c", Side.BUY, 9, 5), T).hasPrice());
        }
    }

    @Nested
    class Fill {

        @Test
        void partialThenFullFill() {
            Order o = new Order(1, OrderRequest.limit("a", "c", Side.BUY, 100, 10), T);
            o.fill(3);
            assertEquals(3, o.getFilledQuantity());
            assertEquals(7, o.getRemainingQuantity());
            o.fill(7);
            assertEquals(0, o.getRemainingQuantity());
        }

        @Test
        void cannotOverfill() {
            Order o = new Order(1, OrderRequest.limit("a", "c", Side.BUY, 100, 10), T);
            o.fill(4);
            assertThrows(IllegalArgumentException.class, () -> o.fill(7));
            assertEquals(6, o.getRemainingQuantity(), "a rejected fill changes nothing");
        }

        @Test
        void fillMustBePositive() {
            Order o = new Order(1, OrderRequest.limit("a", "c", Side.BUY, 100, 10), T);
            assertThrows(IllegalArgumentException.class, () -> o.fill(0));
            assertThrows(IllegalArgumentException.class, () -> o.fill(-1));
        }
    }
}
