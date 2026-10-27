package exchangeserver.book;

import exchangeserver.domain.Order;
import exchangeserver.domain.Side;

import java.util.ArrayList;
import java.util.List;

/*

Orders resting at one price on one side, in time priority, as a doubly linked list with sentinels (D6, D7):

    head <-> o1 <-> o2 <-> ... <-> tail

Every linked node has a non-null prev and next (at worst a sentinel), so link/unlink need no edge branches.

Public: read-only state (price, volume, count). Package-private: every mutation. Only OrderBook mutates a
level, so the engine can read levels but cannot change them (Principle 2; closes the fillHeadOrder tension).

Invariants:
1. totalVolume == sum of remaining quantity over the linked nodes.
2. orderCount  == number of linked nodes.
3. node.level == this  <=>  node is linked in this level.
4. Every linked order has remaining > 0, except the head between fillHead() exhausting it and
   OrderBook unlinking it (both inside one OrderBook.fillBest call).

 */
public final class PriceLevel {
    private final Side side;
    private final long price;
    private final OrderNode head = OrderNode.sentinel();
    private final OrderNode tail = OrderNode.sentinel();
    private long totalVolume;
    private int orderCount;

    PriceLevel(Side side, long price) {
        this.side = side;
        this.price = price;
        head.next = tail;
        tail.prev = head;
    }

    public Side getSide() {
        return side;
    }

    public long getPrice() {
        return price;
    }

    public long getTotalVolume() {
        return totalVolume;
    }

    public int getOrderCount() {
        return orderCount;
    }

    public boolean isEmpty() {
        return head.next == tail;
    }

    // Appends at the tail (time priority). Returns the node so OrderBook can map id -> node for O(1) cancel.
    OrderNode addOrder(Order order) {
        // All checks before any mutation.
        if (order.getSide() != side || order.getPrice() != price) {
            throw new IllegalStateException("Order " + order.getOrderId() + " does not belong in level " + side + " " + price);
        }
        if (order.getRemainingQuantity() <= 0) {
            throw new IllegalStateException("Order " + order.getOrderId() + " has nothing left to rest");
        }
        if (order.getRemainingQuantity() > Long.MAX_VALUE - totalVolume) {
            throw new IllegalStateException("Volume of level " + side + " " + price + " would overflow");
        }

        OrderNode node = new OrderNode(order);
        OrderNode last = tail.prev;
        node.prev = last;
        node.next = tail;
        last.next = node;
        tail.prev = node;
        node.level = this;

        totalVolume += order.getRemainingQuantity();
        orderCount++;
        return node;
    }

    // The node with time priority, or null if the level is empty.
    OrderNode headNode() {
        return head.next == tail ? null : head.next;
    }

    // The node after `node` in time priority, or null at the end. `node` must be linked here.
    OrderNode nextNode(OrderNode node) {
        if (node.level != this) {
            throw new IllegalStateException("Order " + node.order.getOrderId() + " is not linked in level " + side + " " + price);
        }
        return node.next == tail ? null : node.next;
    }

    // Fills the head order by up to maxQuantity and returns how much was filled. Never removes the head:
    // that is OrderBook's job, so the id map and the list change together (D3).
    long fillHead(long maxQuantity) {
        if (maxQuantity <= 0) {
            throw new IllegalArgumentException("Fill quantity must be positive");
        }
        OrderNode first = headNode();
        if (first == null) {
            throw new IllegalStateException("Fill on empty level " + side + " " + price);
        }
        Order order = first.order;
        if (order.getRemainingQuantity() == 0) {
            throw new IllegalStateException("Exhausted order " + order.getOrderId() + " left at the head of " + side + " " + price);
        }

        long quantity = Math.min(order.getRemainingQuantity(), maxQuantity);
        order.fill(quantity);
        totalVolume -= quantity;
        return quantity;
    }

    // O(1). Used for fill-removal (remaining 0) and cancel (remaining > 0) alike (D2).
    void unlink(OrderNode node) {
        if (node.order == null) {
            throw new IllegalStateException("Cannot unlink a sentinel");
        }
        if (node.level != this) {
            throw new IllegalStateException("Order " + node.order.getOrderId() + " is not linked in level " + side + " " + price);
        }

        node.prev.next = node.next;
        node.next.prev = node.prev;

        // 0 for a filled order (fillHead already subtracted it); the live remainder for a cancel.
        totalVolume -= node.order.getRemainingQuantity();
        orderCount--;

        node.prev = null;
        node.next = null;
        node.level = null;
    }

    // For tests: walks next pointers / prev pointers, so tests can check one against the other.
    List<Order> ordersHeadToTail() {
        List<Order> result = new ArrayList<>();
        for (OrderNode n = head.next; n != tail; n = n.next) {
            result.add(n.order);
        }
        return result;
    }

    List<Order> ordersTailToHead() {
        List<Order> result = new ArrayList<>();
        for (OrderNode n = tail.prev; n != head; n = n.prev) {
            result.add(n.order);
        }
        return result;
    }

    @Override
    public String toString() {
        return side + " " + price + " x " + totalVolume + " (" + orderCount + ")";
    }
}
