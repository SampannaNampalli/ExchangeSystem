package exchangeserver.engine;

import exchangeserver.book.CancelResult;
import exchangeserver.book.Fill;
import exchangeserver.book.LevelUpdate;
import exchangeserver.book.OrderBook;
import exchangeserver.book.PriceLevel;
import exchangeserver.book.RestingOrderView;
import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.Order;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Rules;
import exchangeserver.domain.Side;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/*

Price-time-priority matching for one instrument. Decides what a match means; the book carries it out.

Deterministic (D12): behaviour depends only on the commands given, in order. No clock, no counter, no
randomness, no hash-iteration order (the maps here are only looked up; walks go through the book's levels).

Processing a new order (D24):
  1. Checks that change nothing: duplicate client order id, price band, resting caps (only for an order that would
     rest, D68), FOK fillability, post-only cross. A failed check returns before any mutation.
  2. The shared matching loop. It stops at the participant's own resting order (self-trade prevention, D67).
  3. The outcome, as a switch over OrderType with no default (the compiler rejects a missed type).

Not thread-safe: owned by the writer thread.

 */
public final class MatchingEngine {

    private record ClientOrderKey(String participant, String clientOrderId) {
    }

    // How much an order could trade right now, and whether its own resting order is in the way (D67).
    private record Fillability(long quantity, boolean blockedBySelf) {
    }

    // The resting caps (D68). Always Rules' values in production; a smaller pair lets tests reach BOOK_FULL
    // without a million orders.
    record Limits(int maxOpenOrdersPerParticipant, int maxRestingOrders) {
        static final Limits RULES = new Limits(Rules.MAX_OPEN_ORDERS_PER_PARTICIPANT, Rules.MAX_RESTING_ORDERS);
    }

    private final OrderBook book;
    private final Limits limits;
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
        this(book, Limits.RULES);
    }

    MatchingEngine(OrderBook book, Limits limits) {
        if (book.restingOrderCount() != 0) {
            throw new IllegalArgumentException("The engine must start from an empty book");
        }
        this.book = book;
        this.limits = limits;
    }

    // An engine that continues from a snapshot (D71). The orders go back in the given order, which is book
    // priority, so time priority survives; only their client order ids are remembered.
    public static MatchingEngine restore(EngineState state) {
        MatchingEngine engine = new MatchingEngine();
        for (RestingOrderView v : state.orders()) {
            if (v.orderId() > state.lastOrderId()) {
                throw new IllegalStateException("Snapshot order " + v.orderId() + " is above the last order id " + state.lastOrderId());
            }
            OrderRequest request = OrderRequest.of(v.participant(), v.clientOrderId(), v.side(), v.orderType(),
                    v.originalQuantity(), OptionalLong.of(v.price()));
            Order order = new Order(v.orderId(), request, v.timestamp());
            if (v.filledQuantity() > 0) {
                order.fill(v.filledQuantity());
            }
            engine.book.add(order);
            if (engine.orderIdByClientOrderId.putIfAbsent(new ClientOrderKey(v.participant(), v.clientOrderId()), v.orderId()) != null) {
                throw new IllegalStateException("Snapshot repeats client order id " + v.participant() + "/" + v.clientOrderId());
            }
        }
        engine.book.drainLevelUpdates();
        engine.lastOrderId = state.lastOrderId();
        engine.lastSequenceNumber = state.lastSequenceNumber();
        engine.lastTradePrice = state.lastTradePrice();
        return engine;
    }

    public EngineState exportState() {
        return new EngineState(lastSequenceNumber, lastOrderId, lastTradePrice, book.restingOrders());
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

    // Cancels every resting order of the participant, in book priority order (D72).
    public CancelAllOutput onCancelAll(CancelAllCommand command) {
        checkSequence(command.sequenceNumber());
        lastSequenceNumber = command.sequenceNumber();

        List<Long> ids = new ArrayList<>();
        for (Side side : Side.values()) {
            book.walk(side, o -> {
                if (o.participant().equals(command.participant())) {
                    ids.add(o.orderId());
                }
                return true;
            });
        }
        List<CancelResult> results = new ArrayList<>(ids.size());
        for (long id : ids) {
            results.add(book.cancel(command.participant(), id));
        }
        return new CancelAllOutput(command.sequenceNumber(), command.timestamp(), command.participant(), results,
                book.drainLevelUpdates());
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
        OrderType type = order.getOrderType();

        // 1. Checks that change nothing (except remembering the client order id, which every order uses up).
        Long original = orderIdByClientOrderId.putIfAbsent(
                new ClientOrderKey(order.getParticipant(), order.getClientOrderId()), order.getOrderId());
        if (original != null) {
            return rejected(original, order, Reason.DUPLICATE_CLIENT_ORDER_ID);
        }
        if (order.hasPrice() && lastTradePrice > 0 && !Rules.withinBand(order.getPrice(), lastTradePrice)) {
            return rejected(order.getOrderId(), order, Reason.PRICE_OUTSIDE_BAND);
        }

        long limit = matchLimit(order);
        if (type.canRest()) {
            // D68: the caps bound what rests, so an order that will fully trade now is not refused.
            boolean atParticipantCap = book.openOrders(order.getParticipant()) >= limits.maxOpenOrdersPerParticipant();
            boolean bookFull = book.restingOrderCount() >= limits.maxRestingOrders();
            if ((atParticipantCap || bookFull)
                    && (type == OrderType.POST_ONLY || fillability(order, limit).quantity() < order.getRemainingQuantity())) {
                return rejected(order.getOrderId(), order, atParticipantCap ? Reason.MAX_OPEN_ORDERS : Reason.BOOK_FULL);
            }
        }
        if (type == OrderType.FOK) {
            Fillability f = fillability(order, limit);
            if (f.quantity() < order.getRemainingQuantity()) {
                return new Response(order.getOrderId(), order.getClientOrderId(), OrderStatus.CANCELLED,
                        f.blockedBySelf() ? Reason.SELF_TRADE_PREVENTED : null, 0, order.getRemainingQuantity(), List.of());
            }
        }
        if (type == OrderType.POST_ONLY) {
            Optional<PriceLevel> best = book.bestLevel(restingSide);
            if (best.isPresent() && crosses(order.getSide(), limit, best.get().getPrice())) {
                return rejected(order.getOrderId(), order, Reason.POST_ONLY_WOULD_CROSS);
            }
        }

        // 2. Match. A post-only order that got here crosses nothing, so the loop ends at once.
        List<Trade> trades = new ArrayList<>();
        boolean selfTradePrevented = false;
        while (order.getRemainingQuantity() > 0) {
            Optional<PriceLevel> best = book.bestLevel(restingSide);
            if (best.isEmpty() || !crosses(order.getSide(), limit, best.get().getPrice())) {
                break;
            }
            if (order.getParticipant().equals(book.headParticipant(restingSide))) {
                selfTradePrevented = true;      // D67: stop here; the remainder is cancelled below
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

        // 3. Outcome. A remainder stopped by self-trade prevention never rests: it would cross its own order.
        long remaining = order.getRemainingQuantity();
        Reason reason = selfTradePrevented ? Reason.SELF_TRADE_PREVENTED : null;    // the loop only stops there with a remainder
        OrderStatus status = switch (type) {
            case LIMIT, POST_ONLY -> {
                if (remaining == 0) {
                    yield OrderStatus.FILLED;
                }
                if (selfTradePrevented) {
                    yield OrderStatus.CANCELLED;
                }
                book.add(order);
                yield OrderStatus.RESTED;
            }
            case IOC, MARKET -> remaining > 0 ? OrderStatus.CANCELLED : OrderStatus.FILLED;
            case FOK -> {
                if (remaining != 0) {
                    throw new IllegalStateException("Fillable FOK " + order.getOrderId() + " left " + remaining);
                }
                yield OrderStatus.FILLED;
            }
        };
        return new Response(order.getOrderId(), order.getClientOrderId(), status, reason,
                order.getFilledQuantity(), remaining, trades);
    }

    private static Response rejected(long orderId, Order order, Reason reason) {
        return new Response(orderId, order.getClientOrderId(), OrderStatus.REJECTED, reason,
                0, order.getRemainingQuantity(), List.of());
    }

    // The worst price this order may trade at. A priced order: its price. A market order: the collar, fixed at
    // arrival so the order's own trades cannot move it (D50). The collar's reference is the last trade; before the
    // first trade it is the best price on the side the order trades against (D66), so the first market order
    // cannot sweep a thin book. floor / ceil keep the collar exactly equal to Rules.withinBand.
    long matchLimit(Order order) {
        if (order.hasPrice()) {
            return order.getPrice();
        }
        long reference = lastTradePrice;
        if (reference == 0) {
            Optional<PriceLevel> touch = book.bestLevel(order.getSide().opposite());
            if (touch.isEmpty()) {
                return order.getSide() == Side.BUY ? Long.MAX_VALUE : 0;     // nothing to trade with anyway
            }
            reference = touch.get().getPrice();
        }
        return order.getSide() == Side.BUY
                ? reference * (100 + Rules.PRICE_BAND_PERCENT) / 100
                : (reference * (100 - Rules.PRICE_BAND_PERCENT) + 99) / 100;
    }

    // The one definition of "crosses" (D28). A buy crosses an ask at or below its limit; a sell crosses a bid
    // at or above it.
    static boolean crosses(Side incomingSide, long limit, long restingPrice) {
        return incomingSide == Side.BUY ? restingPrice <= limit : restingPrice >= limit;
    }

    // Walks the crossing resting orders in priority order, changes nothing. Stops at the order's own resting
    // order, as the matching loop would (D67).
    private Fillability fillability(Order order, long limit) {
        long needed = order.getRemainingQuantity();
        long[] quantity = {0};
        boolean[] self = {false};
        book.walk(order.getSide().opposite(), resting -> {
            if (!crosses(order.getSide(), limit, resting.price())) {
                return false;
            }
            if (resting.participant().equals(order.getParticipant())) {
                self[0] = true;
                return false;
            }
            quantity[0] += resting.remainingQuantity();
            return quantity[0] < needed;
        });
        return new Fillability(Math.min(quantity[0], needed), self[0]);
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
