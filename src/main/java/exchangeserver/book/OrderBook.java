package exchangeserver.book;

import exchangeserver.domain.Order;
import exchangeserver.domain.Side;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

/*

The order book: owns the lifecycle of every resting order (D1).

Every mutation goes through four methods (add, fillBest, cancel, and the private removeNode they share), so
these invariants have exactly one place to hold:
1. An id is in `orders` iff its order is linked in a level.
2. No empty level is ever left in `bids` or `asks` (eager removal, D10).
3. openOrders.get(p) == number of resting orders owned by p (absent when 0).
4. Every level touched since the last drainLevelUpdates() is in `changed`.

Not thread-safe, on purpose: one writer thread owns it (Vision 5).

Complexity: add O(log L) (level lookup); cancel and fill-removal of a node O(1), plus O(log L) when the level
empties; best-level lookup O(log L).

 */
public final class OrderBook {

    private record LevelKey(Side side, long price) {
    }

    private final TreeMap<Long, PriceLevel> bids = new TreeMap<>(Comparator.reverseOrder());
    private final TreeMap<Long, PriceLevel> asks = new TreeMap<>();
    private final Map<Long, OrderNode> orders = new HashMap<>();
    private final Map<String, Integer> openOrders = new HashMap<>();
    // Insertion order is the order levels were touched in, so drained updates are deterministic.
    private final Set<LevelKey> changed = new LinkedHashSet<>();

    // Rests an order. All-or-nothing: if it throws, the book is unchanged.
    public void add(Order order) {
        if (!order.getOrderType().canRest()) {
            throw new IllegalStateException("A " + order.getOrderType() + " order never rests: " + order.getOrderId());
        }
        if (order.getRemainingQuantity() <= 0) {
            throw new IllegalStateException("Order " + order.getOrderId() + " has nothing left to rest");
        }
        if (orders.containsKey(order.getOrderId())) {
            throw new IllegalStateException("Duplicate order id " + order.getOrderId());       // D15
        }

        TreeMap<Long, PriceLevel> side = sideOf(order.getSide());
        PriceLevel level = side.get(order.getPrice());
        boolean newLevel = level == null;
        if (newLevel) {
            level = new PriceLevel(order.getSide(), order.getPrice());
        }
        OrderNode node = level.addOrder(order);         // throws before linking anything
        if (newLevel) {
            side.put(order.getPrice(), level);          // only now: a failed add must not leave an empty level
        }

        orders.put(order.getOrderId(), node);
        openOrders.merge(order.getParticipant(), 1, Integer::sum);
        changed.add(new LevelKey(order.getSide(), order.getPrice()));
    }

    // Fills the order with time priority at the best level of `side` by up to maxQuantity. An exhausted order
    // leaves the book here, and an emptied level with it, so the engine never touches book internals (D4, D14).
    public Fill fillBest(Side side, long maxQuantity) {
        Map.Entry<Long, PriceLevel> best = sideOf(side).firstEntry();
        if (best == null) {
            throw new IllegalStateException("Fill on empty side " + side);
        }
        PriceLevel level = best.getValue();
        OrderNode head = level.headNode();
        Order order = head.order;

        long quantity = level.fillHead(maxQuantity);
        changed.add(new LevelKey(side, level.getPrice()));
        if (order.getRemainingQuantity() == 0) {
            removeNode(head);
        }

        return new Fill(order.getOrderId(), order.getParticipant(), order.getClientOrderId(), side,
                level.getPrice(), quantity, order.getRemainingQuantity());
    }

    // Only the owner can cancel. Someone else's order looks exactly like a missing one (D48).
    public CancelResult cancel(String participant, long orderId) {
        OrderNode node = orders.get(orderId);
        if (node == null || !node.order.getParticipant().equals(participant)) {
            return CancelResult.unknown(orderId);
        }

        long remaining = node.order.getRemainingQuantity();
        changed.add(new LevelKey(node.order.getSide(), node.order.getPrice()));
        removeNode(node);
        return CancelResult.cancelled(orderId, remaining);
    }

    // The single removal path for fills and cancels (D2).
    private void removeNode(OrderNode node) {
        Order order = node.order;
        PriceLevel level = node.level;
        if (level == null || orders.get(order.getOrderId()) != node) {
            throw new IllegalStateException("Order " + order.getOrderId() + " is not resting");
        }

        level.unlink(node);
        orders.remove(order.getOrderId());
        openOrders.computeIfPresent(order.getParticipant(), (p, n) -> n == 1 ? null : n - 1);

        if (level.isEmpty()) {
            PriceLevel removed = sideOf(level.getSide()).remove(level.getPrice());
            if (removed != level) {
                throw new IllegalStateException("Removed the wrong level at " + level.getSide() + " " + level.getPrice());   // D18
            }
        }
    }

    // The updates for every level touched since the last call, in the order they were first touched.
    public List<LevelUpdate> drainLevelUpdates() {
        if (changed.isEmpty()) {
            return List.of();
        }
        List<LevelUpdate> updates = new ArrayList<>(changed.size());
        for (LevelKey key : changed) {
            PriceLevel level = sideOf(key.side()).get(key.price());
            updates.add(level == null
                    ? new LevelUpdate(key.side(), key.price(), 0, 0)
                    : new LevelUpdate(key.side(), key.price(), level.getTotalVolume(), level.getOrderCount()));
        }
        changed.clear();
        return updates;
    }

    // Visits the resting orders of `side` in priority order (best level first, time priority within a level) until
    // the visitor returns false. Read-only: the visitor gets copies. Deterministic: it walks the levels, never a
    // hash map.
    public void walk(Side side, Predicate<RestingOrderView> visitor) {
        for (PriceLevel level : sideOf(side).values()) {
            for (OrderNode n = level.headNode(); n != null; n = level.nextNode(n)) {
                if (!visitor.test(view(n.order))) {
                    return;
                }
            }
        }
    }

    // Every resting order: bids best first, then asks best first, each level in time priority.
    public List<RestingOrderView> restingOrders() {
        List<RestingOrderView> all = new ArrayList<>(orders.size());
        walk(Side.BUY, all::add);
        walk(Side.SELL, all::add);
        return all;
    }

    // The owner of the order that would trade next on `side`, or null if that side is empty (D67).
    public String headParticipant(Side side) {
        Map.Entry<Long, PriceLevel> best = sideOf(side).firstEntry();
        return best == null ? null : best.getValue().headNode().order.getParticipant();
    }

    private static RestingOrderView view(Order o) {
        return new RestingOrderView(o.getOrderId(), o.getParticipant(), o.getClientOrderId(), o.getSide(),
                o.getOrderType(), o.getPrice(), o.getOriginalQuantity(), o.getFilledQuantity(), o.getTimestamp());
    }

    // `side` is the side of the book itself, not the side of an incoming order.
    public Optional<PriceLevel> bestLevel(Side side) {
        Map.Entry<Long, PriceLevel> best = sideOf(side).firstEntry();
        return best == null ? Optional.empty() : Optional.of(best.getValue());
    }

    // Best first. Read-only: the view rejects removal, and PriceLevel exposes no public mutators.
    public Collection<PriceLevel> levels(Side side) {
        return Collections.unmodifiableCollection(sideOf(side).values());
    }

    public long volumeAt(Side side, long price) {
        PriceLevel level = sideOf(side).get(price);
        return level == null ? 0 : level.getTotalVolume();
    }

    public int levelCount(Side side) {
        return sideOf(side).size();
    }

    public int restingOrderCount() {
        return orders.size();
    }

    public boolean isResting(long orderId) {
        return orders.containsKey(orderId);
    }

    public int openOrders(String participant) {
        return openOrders.getOrDefault(participant, 0);
    }

    // For tests: the orders at one level in time priority, checked against the reverse walk.
    List<Order> ordersAt(Side side, long price) {
        PriceLevel level = sideOf(side).get(price);
        if (level == null) {
            return List.of();
        }
        List<Order> forward = level.ordersHeadToTail();
        List<Order> backward = level.ordersTailToHead();
        Collections.reverse(backward);
        if (!forward.equals(backward)) {
            throw new IllegalStateException("prev and next pointers disagree at " + side + " " + price);
        }
        return forward;
    }

    // For tests: checks every invariant of the book and its levels from scratch. O(n).
    void checkInvariants() {
        Map<String, Integer> owned = new HashMap<>();
        int linked = 0;
        for (Side side : Side.values()) {
            long previousPrice = 0;
            for (PriceLevel level : sideOf(side).values()) {
                List<Order> resting = ordersAt(side, level.getPrice());
                if (resting.isEmpty()) {
                    throw new IllegalStateException("Empty level " + level);
                }
                if (previousPrice != 0 && (side == Side.BUY ? level.getPrice() >= previousPrice : level.getPrice() <= previousPrice)) {
                    throw new IllegalStateException("Levels out of order at " + level);
                }
                previousPrice = level.getPrice();
                long volume = 0;
                for (Order o : resting) {
                    if (o.getRemainingQuantity() <= 0 || o.getSide() != side || o.getPrice() != level.getPrice()) {
                        throw new IllegalStateException("Bad order " + o + " in " + level);
                    }
                    OrderNode node = orders.get(o.getOrderId());
                    if (node == null || node.order != o || node.level != level) {
                        throw new IllegalStateException("Id map disagrees for " + o);
                    }
                    volume += o.getRemainingQuantity();
                    owned.merge(o.getParticipant(), 1, Integer::sum);
                }
                if (volume != level.getTotalVolume() || resting.size() != level.getOrderCount()) {
                    throw new IllegalStateException("Totals wrong at " + level + ": volume " + volume + ", orders " + resting.size());
                }
                linked += resting.size();
            }
        }
        if (linked != orders.size()) {
            throw new IllegalStateException("Id map has " + orders.size() + " entries, levels hold " + linked);
        }
        if (!owned.equals(openOrders)) {
            throw new IllegalStateException("Open-order counts " + openOrders + ", actual " + owned);
        }
        if (!bids.isEmpty() && !asks.isEmpty() && bids.firstKey() >= asks.firstKey()) {
            throw new IllegalStateException("Crossed book: bid " + bids.firstKey() + " >= ask " + asks.firstKey());
        }
    }

    private TreeMap<Long, PriceLevel> sideOf(Side side) {
        return side == Side.BUY ? bids : asks;
    }
}
