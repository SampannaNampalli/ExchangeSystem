package exchangeserver.book;

import exchangeserver.domain.Order;

import java.util.Objects;

/*

A node of the doubly linked list inside a PriceLevel (D5, D6).
Book layer only: PriceLevel links and unlinks it, OrderBook holds it in its id map.

order == null  <=>  sentinel. Enforced by construction: the real-node constructor rejects null, and
sentinels can only come from sentinel().

level == the PriceLevel  <=>  the node is linked in that level. Null for sentinels and unlinked nodes (D8).

 */
final class OrderNode {
    final Order order;
    OrderNode prev;
    OrderNode next;
    PriceLevel level;

    OrderNode(Order order) {
        this.order = Objects.requireNonNull(order, "A real node must wrap an order");
    }

    private OrderNode() {
        this.order = null;
    }

    static OrderNode sentinel() {
        return new OrderNode();
    }
}
