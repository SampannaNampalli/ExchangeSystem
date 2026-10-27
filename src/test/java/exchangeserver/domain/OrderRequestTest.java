package exchangeserver.domain;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderRequestTest {

    @Nested
    class Factories {

        @Test
        void limitKeepsEveryField() {
            OrderRequest r = OrderRequest.limit("alice", "c1", Side.BUY, 100, 10);
            assertEquals("alice", r.getParticipant());
            assertEquals("c1", r.getClientOrderId());
            assertEquals(Side.BUY, r.getSide());
            assertEquals(OrderType.LIMIT, r.getOrderType());
            assertEquals(10, r.getQuantity());
            assertEquals(OptionalLong.of(100), r.getPrice());
        }

        @Test
        void eachTypedFactorySetsItsType() {
            assertEquals(OrderType.IOC, OrderRequest.ioc("a", "c", Side.SELL, 5, 1).getOrderType());
            assertEquals(OrderType.FOK, OrderRequest.fok("a", "c", Side.SELL, 5, 1).getOrderType());
            assertEquals(OrderType.POST_ONLY, OrderRequest.postOnly("a", "c", Side.SELL, 5, 1).getOrderType());
            assertEquals(OrderType.MARKET, OrderRequest.market("a", "c", Side.SELL, 1).getOrderType());
        }

        @Test
        void marketOrderHasNoPrice() {
            assertTrue(OrderRequest.market("a", "c", Side.BUY, 3).getPrice().isEmpty());
        }

        @Test
        void generalFactoryMatchesTypedFactories() {
            assertEquals(OrderRequest.limit("a", "c", Side.BUY, 100, 10),
                    OrderRequest.of("a", "c", Side.BUY, OrderType.LIMIT, 10, OptionalLong.of(100)));
            assertEquals(OrderRequest.market("a", "c", Side.BUY, 10),
                    OrderRequest.of("a", "c", Side.BUY, OrderType.MARKET, 10, OptionalLong.empty()));
        }

        @Test
        void generalFactoryRejectsMarketWithPrice() {
            assertThrows(IllegalArgumentException.class,
                    () -> OrderRequest.of("a", "c", Side.BUY, OrderType.MARKET, 10, OptionalLong.of(100)));
        }

        @Test
        void generalFactoryRejectsPricedTypeWithoutPrice() {
            for (OrderType type : new OrderType[]{OrderType.LIMIT, OrderType.IOC, OrderType.FOK, OrderType.POST_ONLY}) {
                assertThrows(IllegalArgumentException.class,
                        () -> OrderRequest.of("a", "c", Side.BUY, type, 10, OptionalLong.empty()), type.name());
            }
        }
    }

    @Nested
    class Validation {

        @ParameterizedTest
        @ValueSource(longs = {0, -1, Long.MIN_VALUE})
        void quantityMustBePositive(long quantity) {
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit("a", "c", Side.BUY, 100, quantity));
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.market("a", "c", Side.BUY, quantity));
        }

        @Test
        void quantityLimitIsInclusive() {
            OrderRequest.limit("a", "c", Side.BUY, 100, Rules.MAX_ORDER_QUANTITY);
            assertThrows(IllegalArgumentException.class,
                    () -> OrderRequest.limit("a", "c", Side.BUY, 100, Rules.MAX_ORDER_QUANTITY + 1));
        }

        @ParameterizedTest
        @ValueSource(longs = {0, -1, -100})
        void priceMustBePositive(long price) {
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit("a", "c", Side.BUY, price, 1));
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.postOnly("a", "c", Side.BUY, price, 1));
        }

        @Test
        void priceLimitIsInclusive() {
            OrderRequest.limit("a", "c", Side.BUY, Rules.MAX_PRICE, 1);
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit("a", "c", Side.BUY, Rules.MAX_PRICE + 1, 1));
        }

        @Test
        void sideAndTypeAreRequired() {
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit("a", "c", null, 100, 1));
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.of("a", "c", Side.BUY, null, 1, OptionalLong.of(1)));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "has space", "comma,", "seventeen_chars_x", "é"})
        void participantMustMatchPattern(String participant) {
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit(participant, "c", Side.BUY, 100, 1));
        }

        @Test
        void nullParticipantOrClientOrderIdIsRejected() {
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit(null, "c", Side.BUY, 100, 1));
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit("a", null, Side.BUY, 100, 1));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "a b", "a,b", "123456789012345678901234567890123"})
        void clientOrderIdMustMatchPattern(String clientOrderId) {
            assertThrows(IllegalArgumentException.class, () -> OrderRequest.limit("a", clientOrderId, Side.BUY, 100, 1));
        }

        @Test
        void clientOrderIdAllowsDotsDashesUnderscores() {
            OrderRequest.limit("a", "A-1.b_2", Side.BUY, 100, 1);
            OrderRequest.limit("a", "12345678901234567890123456789012", Side.BUY, 100, 1);
        }
    }

    @Test
    void equalityCoversEveryField() {
        OrderRequest base = OrderRequest.limit("a", "c", Side.BUY, 100, 10);
        assertEquals(base, OrderRequest.limit("a", "c", Side.BUY, 100, 10));
        assertEquals(base.hashCode(), OrderRequest.limit("a", "c", Side.BUY, 100, 10).hashCode());
        assertNotEquals(base, OrderRequest.limit("b", "c", Side.BUY, 100, 10));
        assertNotEquals(base, OrderRequest.limit("a", "d", Side.BUY, 100, 10));
        assertNotEquals(base, OrderRequest.limit("a", "c", Side.SELL, 100, 10));
        assertNotEquals(base, OrderRequest.limit("a", "c", Side.BUY, 101, 10));
        assertNotEquals(base, OrderRequest.limit("a", "c", Side.BUY, 100, 11));
        assertNotEquals(base, OrderRequest.ioc("a", "c", Side.BUY, 100, 10));
    }
}
