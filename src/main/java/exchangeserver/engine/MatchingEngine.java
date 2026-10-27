package exchangeserver.engine;

import exchangeserver.book.CancelResult;
import exchangeserver.book.Fill;
import exchangeserver.book.LevelUpdate;
import exchangeserver.book.OrderBook;
import exchangeserver.book.PriceLevel;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.Order;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Rules;
import exchangeserver.domain.Side;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*

Price-time-priority matching for one instrument. Decides what a match means; the book carries it out.

Deterministic (D12): behaviour depends only on the commands given, in order. No clock, no counter, no
randomness, no hash-iteration order (the maps here are only looked up).

Processing a new order (D24):
  1. Checks that change nothing: duplicate client order id, price band, open-order cap, FOK fillability,
     post-only cross. A failed check returns before any mutation.
  2. The shared matching loop.
  3. The outcome, as a switch over OrderType with no default (the compiler rejects a missed type).

Not thread-safe: owned by the writer thread.

 */
public final class MatchingEngine {

    private record ClientOrderKey(String participant, String clientOrderId) {
    }

    private final OrderBook book;
    private final Map<ClientOrderKey, Long> orderIdByClientOrderId = new HashMap<>();
    private long lastOrderId;
    private long lastSequenceNumber;
    private long lastTradePrice;        // 0 = no trade yet; the reference for the band and the collar

    public MatchingEngine() {
        this(new OrderBook());
    }

    // For tests that inspect the book. Production code (Exchange) uses the no-argument constructor, so no
    // caller keeps a reference that could change the book behind the engine's back.
    public MatchingEngine(OrderBook book) {
        if (book.restingOrderCount() != 0) {
            throw new IllegalArgumentException("The engine must start from an empty book");
        }
        this.book = book;
    }

    public OrderOutput onNewOrder(NewOrderCommand command) {
        // D35: catches any id reuse, including ids that already left the book. Before any mutation.
        if (command.orderId() <= lastOrderId) {
            throw new IllegalStateException("Order id " + command.orderId() + " is not above the last one, " + lastOrderId);
        }
        checkSequence(command.sequenceNumber());
        lastOrderId = command.orderId();
        lastSequenceNumber = command.sequenceNumber();

        Order order = new Order(command.orderId(), command.request(), command.timestamp());
        List<Execution> passiveFills = new ArrayList<>();
        Response response = process(order, passiveFills);
        return new OrderOutput(command.sequenceNumber(), command.timestamp(), response, passiveFills,
                book.drainLevelUpdates());
    }

    public CancelOutput onCancel(CancelOrderCommand command) {
        checkSequence(command.sequenceNumber());
        lastSequenceNumber = command.sequenceNumber();

        CancelResult result = book.cancel(command.participant(), command.orderId());
        return new CancelOutput(command.sequenceNumber(), command.timestamp(), result, book.drainLevelUpdates());
    }

    // Up to `depth` levels per side, best first.
    public BookSnapshot snapshot(int depth) {
        if (depth <= 0) {
            throw new IllegalArgumentException("Depth must be positive");
        }
        return new BookSnapshot(lastSequenceNumber, lastTradePrice, levels(Side.BUY, depth), levels(Side.SELL, depth));
    }

    public long lastTradePrice() {
        return lastTradePrice;
    }

    public long lastSequenceNumber() {
        return lastSequenceNumber;
    }

    // For tests in this package. Reading it from another thread is a data race (step 4).
    OrderBook book() {
        return book;
    }

    private Response process(Order order, List<Execution> passiveFills) {
        Side restingSide = order.getSide().opposite();

        // 1. Checks that change nothing (except remembering the client order id, which every order uses up).
        Long original = orderIdByClientOrderId.putIfAbsent(
                new ClientOrderKey(order.getParticipant(), order.getClientOrderId()), order.getOrderId());
        if (original != null) {
            return rejected(original, order, RejectReason.DUPLICATE_CLIENT_ORDER_ID);
        }
        if (order.hasPrice() && lastTradePrice > 0 && !Rules.withinBand(order.getPrice(), lastTradePrice)) {
            return rejected(order.getOrderId(), order, RejectReason.PRICE_OUTSIDE_BAND);
        }
        if (order.getOrderType().canRest()
                && book.openOrders(order.getParticipant()) >= Rules.MAX_OPEN_ORDERS_PER_PARTICIPANT) {
            return rejected(order.getOrderId(), order, RejectReason.MAX_OPEN_ORDERS);
        }

        long limit = matchLimit(order);
        if (order.getOrderType() == OrderType.FOK && !canBeFilled(order, limit)) {
            return new Response(order.getOrderId(), order.getClientOrderId(), OrderStatus.CANCELLED, null,
                    0, order.getRemainingQuantity(), List.of());
        }
        if (order.getOrderType() == OrderType.POST_ONLY) {
            Optional<PriceLevel> best = book.bestLevel(restingSide);
            if (best.isPresent() && crosses(order.getSide(), limit, best.get().getPrice())) {
                return rejected(order.getOrderId(), order, RejectReason.POST_ONLY_WOULD_CROSS);
            }
        }

        // 2. Match. A post-only order that got here crosses nothing, so the loop ends at once.
        List<Trade> trades = new ArrayList<>();
        while (order.getRemainingQuantity() > 0) {
            Optional<PriceLevel> best = book.bestLevel(restingSide);
            if (best.isEmpty() || !crosses(order.getSide(), limit, best.get().getPrice())) {
                break;
            }
            Fill fill = book.fillBest(restingSide, order.getRemainingQuantity());
            order.fill(fill.quantity());
            lastTradePrice = fill.price();

            long buyId = order.getSide() == Side.BUY ? order.getOrderId() : fill.restingOrderId();
            long sellId = order.getSide() == Side.BUY ? fill.restingOrderId() : order.getOrderId();
            trades.add(new Trade(fill.price(), fill.quantity(), buyId, sellId, order.getSide(), order.getTimestamp()));
            passiveFills.add(new Execution(fill.participant(), fill.restingOrderId(), fill.clientOrderId(),
                    restingSide, fill.price(), fill.quantity(), fill.leavesQuantity()));
        }

        // 3. Outcome.
        long remaining = order.getRemainingQuantity();
        OrderStatus status = switch (order.getOrderType()) {
            case LIMIT, POST_ONLY -> {
                if (remaining > 0) {
                    book.add(order);
                    yield OrderStatus.RESTED;
                }
                yield OrderStatus.FILLED;
            }
            case IOC, MARKET -> remaining > 0 ? OrderStatus.CANCELLED : OrderStatus.FILLED;
            case FOK -> {
                if (remaining != 0) {
                    throw new IllegalStateException("Fillable FOK " + order.getOrderId() + " left " + remaining);
                }
                yield OrderStatus.FILLED;
            }
        };
        return new Response(order.getOrderId(), order.getClientOrderId(), status, null,
                order.getFilledQuantity(), remaining, trades);
    }

    private static Response rejected(long orderId, Order order, RejectReason reason) {
        return new Response(orderId, order.getClientOrderId(), OrderStatus.REJECTED, reason,
                0, order.getRemainingQuantity(), List.of());
    }

    // The worst price this order may trade at. A priced order: its price. A market order: the collar around
    // the last trade (D50), fixed at arrival so the order's own trades cannot move it; no trade yet => no limit.
    // floor / ceil keep the collar exactly equal to Rules.withinBand.
    long matchLimit(Order order) {
        if (order.hasPrice()) {
            return order.getPrice();
        }
        if (lastTradePrice == 0) {
            return order.getSide() == Side.BUY ? Long.MAX_VALUE : 0;
        }
        return order.getSide() == Side.BUY
                ? lastTradePrice * (100 + Rules.PRICE_BAND_PERCENT) / 100
                : (lastTradePrice * (100 - Rules.PRICE_BAND_PERCENT) + 99) / 100;
    }

    // The one definition of "crosses" (D28). A buy crosses an ask at or below its limit; a sell crosses a bid
    // at or above it.
    static boolean crosses(Side incomingSide, long limit, long restingPrice) {
        return incomingSide == Side.BUY ? restingPrice <= limit : restingPrice >= limit;
    }

    // FOK pre-check: walks only the crossing levels, changes nothing.
    private boolean canBeFilled(Order order, long limit) {
        long needed = order.getRemainingQuantity();
        for (PriceLevel level : book.levels(order.getSide().opposite())) {
            if (!crosses(order.getSide(), limit, level.getPrice())) {
                return false;
            }
            needed -= level.getTotalVolume();
            if (needed <= 0) {
                return true;
            }
        }
        return false;
    }

    private void checkSequence(long sequenceNumber) {
        if (sequenceNumber <= lastSequenceNumber) {
            throw new IllegalStateException("Sequence number " + sequenceNumber + " is not above the last one, " + lastSequenceNumber);
        }
    }

    private List<LevelUpdate> levels(Side side, int depth) {
        List<LevelUpdate> result = new ArrayList<>();
        for (PriceLevel level : book.levels(side)) {
            if (result.size() == depth) {
                break;
            }
            result.add(new LevelUpdate(side, level.getPrice(), level.getTotalVolume(), level.getOrderCount()));
        }
        return result;
    }
}
