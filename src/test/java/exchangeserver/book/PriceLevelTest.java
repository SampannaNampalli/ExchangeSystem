package exchangeserver.book;

import exchangeserver.domain.Order;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriceLevelTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    private final PriceLevel level = new PriceLevel(Side.SELL, 100);

    private static Order sell(long id, long price, long qty) {
        return new Order(id, OrderRequest.limit("p", "c" + id, Side.SELL, price, qty), T);
    }

    // Both walks must agree, so every test also checks the prev pointers.
    private List<Order> orders() {
        List<Order> forward = level.ordersHeadToTail();
        List<Order> backward = level.ordersTailToHead();
        java.util.Collections.reverse(backward);
        assertEquals(forward, backward, "next and prev walks disagree");
        return forward;
    }

    @Nested
    class AddOrder {

        @Test
        void newLevelIsEmpty() {
            assertTrue(level.isEmpty());
            assertEquals(0, level.getTotalVolume());
            assertEquals(0, level.getOrderCount());
            assertNull(level.headNode());
        }

        @Test
        void appendsInTimePriorityAndKeepsTotals() {
            Order a = sell(1, 100, 5);
            Order b = sell(2, 100, 7);
            Order c = sell(3, 100, 1);
            level.addOrder(a);
            level.addOrder(b);
            level.addOrder(c);
            assertEquals(List.of(a, b, c), orders());
            assertEquals(13, level.getTotalVolume());
            assertEquals(3, level.getOrderCount());
            assertSame(a, level.headNode().order);
        }

        @Test
        void returnsTheLinkedNode() {
            Order a = sell(1, 100, 5);
            OrderNode node = level.addOrder(a);
            assertSame(a, node.order);
            assertSame(level, node.level);
        }

        @Test
        void rejectsWrongPriceOrSideWithoutChange() {
            assertThrows(IllegalStateException.class, () -> level.addOrder(sell(1, 101, 5)));
            Order buy = new Order(2, OrderRequest.limit("p", "c", Side.BUY, 100, 5), T);
            assertThrows(IllegalStateException.class, () -> level.addOrder(buy));
            assertTrue(level.isEmpty());
            assertEquals(0, level.getTotalVolume());
        }

        @Test
        void rejectsAnExhaustedOrder() {
            Order a = sell(1, 100, 5);
            a.fill(5);
            assertThrows(IllegalStateException.class, () -> level.addOrder(a));
            assertTrue(level.isEmpty());
        }

        @Test
        void addsRemainingNotOriginalQuantity() {
            Order a = sell(1, 100, 10);
            a.fill(4);
            level.addOrder(a);
            assertEquals(6, level.getTotalVolume());
        }
    }

    @Nested
    class FillHead {

        @Test
        void partialFillKeepsTheHead() {
            Order a = sell(1, 100, 10);
            level.addOrder(a);
            level.addOrder(sell(2, 100, 5));
            assertEquals(4, level.fillHead(4));
            assertEquals(6, a.getRemainingQuantity());
            assertEquals(11, level.getTotalVolume());
            assertSame(a, level.headNode().order);
        }

        @Test
        void fillsAtMostTheHeadsRemainder() {
            Order a = sell(1, 100, 3);
            level.addOrder(a);
            level.addOrder(sell(2, 100, 5));
            assertEquals(3, level.fillHead(100));
            assertEquals(0, a.getRemainingQuantity());
            assertEquals(5, level.getTotalVolume());
        }

        @Test
        void neverRemovesAnExhaustedHead() {
            Order a = sell(1, 100, 3);
            level.addOrder(a);
            level.fillHead(3);
            assertEquals(1, level.getOrderCount(), "removal is OrderBook's job (D3)");
            assertSame(a, level.headNode().order);
        }

        @Test
        void refusesToFillAnExhaustedHeadAgain() {
            level.addOrder(sell(1, 100, 3));
            level.fillHead(3);
            assertThrows(IllegalStateException.class, () -> level.fillHead(1));
        }

        @Test
        void refusesOnEmptyLevelOrNonPositiveQuantity() {
            assertThrows(IllegalStateException.class, () -> level.fillHead(1));
            level.addOrder(sell(1, 100, 3));
            assertThrows(IllegalArgumentException.class, () -> level.fillHead(0));
            assertThrows(IllegalArgumentException.class, () -> level.fillHead(-2));
            assertEquals(3, level.getTotalVolume());
        }
    }

    @Nested
    class Unlink {

        private Order a;
        private Order b;
        private Order c;
        private OrderNode na;
        private OrderNode nb;
        private OrderNode nc;

        private void addThree() {
            a = sell(1, 100, 1);
            b = sell(2, 100, 2);
            c = sell(3, 100, 4);
            na = level.addOrder(a);
            nb = level.addOrder(b);
            nc = level.addOrder(c);
        }

        @Test
        void middle() {
            addThree();
            level.unlink(nb);
            assertEquals(List.of(a, c), orders());
            assertEquals(5, level.getTotalVolume());
            assertEquals(2, level.getOrderCount());
        }

        @Test
        void head() {
            addThree();
            level.unlink(na);
            assertEquals(List.of(b, c), orders());
            assertSame(b, level.headNode().order);
        }

        @Test
        void tailThenAppend() {
            addThree();
            level.unlink(nc);
            Order d = sell(4, 100, 8);
            level.addOrder(d);
            assertEquals(List.of(a, b, d), orders());
        }

        @Test
        void onlyNodeLeavesAnEmptyLevel() {
            OrderNode n = level.addOrder(sell(1, 100, 5));
            level.unlink(n);
            assertTrue(level.isEmpty());
            assertEquals(0, level.getTotalVolume());
            assertEquals(0, level.getOrderCount());
        }

        @Test
        void subtractsRemainingSoAFilledOrderSubtractsNothing() {
            addThree();
            level.fillHead(1);                 // a exhausted, volume already reduced by 1
            assertEquals(6, level.getTotalVolume());
            level.unlink(na);
            assertEquals(6, level.getTotalVolume());
        }

        @Test
        void clearsTheNodesLinks() {
            addThree();
            level.unlink(nb);
            assertNull(nb.prev);
            assertNull(nb.next);
            assertNull(nb.level);
        }

        @Test
        void refusesASecondUnlink() {
            addThree();
            level.unlink(nb);
            assertThrows(IllegalStateException.class, () -> level.unlink(nb));
            assertEquals(List.of(a, c), orders());
        }

        @Test
        void refusesANodeFromAnotherLevel() {
            addThree();
            PriceLevel other = new PriceLevel(Side.SELL, 101);
            OrderNode foreign = other.addOrder(sell(9, 101, 3));
            assertThrows(IllegalStateException.class, () -> level.unlink(foreign));
            assertEquals(List.of(a, b, c), orders());
            assertEquals(1, other.getOrderCount());
        }

        @Test
        void refusesASentinel() {
            assertThrows(IllegalStateException.class, () -> level.unlink(OrderNode.sentinel()));
        }
    }

    @Test
    void nextNodeWalksInTimePriorityAndRefusesForeignNodes() {
        OrderNode first = level.addOrder(sell(1, 100, 1));
        OrderNode second = level.addOrder(sell(2, 100, 1));
        assertSame(second, level.nextNode(first));
        assertNull(level.nextNode(second));
        level.unlink(first);
        assertThrows(IllegalStateException.class, () -> level.nextNode(first));
    }

    @Test
    void realNodeNeedsAnOrder() {
        assertThrows(NullPointerException.class, () -> new OrderNode(null));
    }
}
