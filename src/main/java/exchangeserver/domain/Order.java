package exchangeserver.domain;

import java.time.Instant;
import java.util.Objects;

/*

An order the exchange has accepted: a validated request plus the sequencer's id and timestamp.

The only constructor takes an OrderRequest, which cannot be invalid, so an invalid Order cannot exist without
repeating the field checks here (D47). What the request cannot know is checked here: the id and the timestamp.

An Order does not know where it is stored (no book, level or side-of-book references).

 */
public final class Order {

    static final long NO_PRICE = -1;

    private final long orderId;
    private final String participant;
    private final String clientOrderId;
    private final Side side;
    private final OrderType orderType;
    private final long price;
    private final long originalQuantity;
    private final Instant timestamp;
    private long filledQuantity;

    public Order(long orderId, OrderRequest request, Instant timestamp) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(timestamp, "timestamp");
        if (orderId <= 0) {
            throw new IllegalArgumentException("Order id must be positive");
        }

        this.orderId = orderId;
        this.participant = request.getParticipant();
        this.clientOrderId = request.getClientOrderId();
        this.side = request.getSide();
        this.orderType = request.getOrderType();
        this.price = request.rawPrice();
        this.originalQuantity = request.getQuantity();
        this.timestamp = timestamp;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getParticipant() {
        return participant;
    }

    public String getClientOrderId() {
        return clientOrderId;
    }

    public Side getSide() {
        return side;
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public boolean hasPrice() {
        return orderType != OrderType.MARKET;
    }

    // A market order has no price; asking for one is a bug.
    public long getPrice() {
        if (!hasPrice()) {
            throw new IllegalStateException("Order " + orderId + " is a market order and has no price");
        }
        return price;
    }

    public long getOriginalQuantity() {
        return originalQuantity;
    }

    public long getFilledQuantity() {
        return filledQuantity;
    }

    public long getRemainingQuantity() {
        return originalQuantity - filledQuantity;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void fill(long quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Fill quantity must be positive");
        }
        if (quantity > getRemainingQuantity()) {
            throw new IllegalArgumentException("Cannot overfill order " + orderId);
        }
        filledQuantity += quantity;
    }

    @Override
    public String toString() {
        return "Order{" + orderId + " " + orderType + " " + side + " " + getRemainingQuantity() + "/" + originalQuantity
                + (hasPrice() ? " @ " + price : "") + " " + participant + "/" + clientOrderId + "}";
    }
}
