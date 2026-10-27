package exchangeserver.domain;

import java.util.Objects;
import java.util.OptionalLong;

/*

A client's order before the exchange has stamped it: the stateless validation layer (D19).
Built at the gateway, and again by the journal's decoder (reuse, not a third copy of the rules).

A market order has no price. The constructor is private and every factory takes a price only where one
exists, so the in-memory "no price" sentinel never reaches a caller.

 */
public final class OrderRequest {

    private final String participant;
    private final String clientOrderId;
    private final Side side;
    private final OrderType orderType;
    private final long quantity;
    private final long price;       // Order.NO_PRICE for MARKET

    private OrderRequest(String participant, String clientOrderId, Side side, OrderType orderType, long quantity, long price) {
        if (participant == null || !Rules.PARTICIPANT.matcher(participant).matches()) {
            throw new IllegalArgumentException("Invalid participant: " + participant);
        }
        if (clientOrderId == null || !Rules.CLIENT_ORDER_ID.matcher(clientOrderId).matches()) {
            throw new IllegalArgumentException("Invalid client order id: " + clientOrderId);
        }
        if (side == null) {
            throw new IllegalArgumentException("Side is required");
        }
        if (orderType == null) {
            throw new IllegalArgumentException("Order type is required");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be positive");
        }
        if (quantity > Rules.MAX_ORDER_QUANTITY) {
            throw new IllegalArgumentException("Quantity above the maximum of " + Rules.MAX_ORDER_QUANTITY);
        }
        if (orderType != OrderType.MARKET) {
            if (price <= 0) {
                throw new IllegalArgumentException("Price must be positive");
            }
            if (price > Rules.MAX_PRICE) {
                throw new IllegalArgumentException("Price above the maximum of " + Rules.MAX_PRICE);
            }
        }

        this.participant = participant;
        this.clientOrderId = clientOrderId;
        this.side = side;
        this.orderType = orderType;
        this.quantity = quantity;
        this.price = price;
    }

    // The general factory: the price is present for every type except MARKET.
    public static OrderRequest of(String participant, String clientOrderId, Side side, OrderType orderType,
                                  long quantity, OptionalLong price) {
        Objects.requireNonNull(price, "price");
        if (orderType == OrderType.MARKET) {
            if (price.isPresent()) {
                throw new IllegalArgumentException("A market order has no price");
            }
            return new OrderRequest(participant, clientOrderId, side, orderType, quantity, Order.NO_PRICE);
        }
        if (price.isEmpty()) {
            throw new IllegalArgumentException("A " + orderType + " order needs a price");
        }
        return new OrderRequest(participant, clientOrderId, side, orderType, quantity, price.getAsLong());
    }

    public static OrderRequest limit(String participant, String clientOrderId, Side side, long price, long quantity) {
        return new OrderRequest(participant, clientOrderId, side, OrderType.LIMIT, quantity, price);
    }

    public static OrderRequest ioc(String participant, String clientOrderId, Side side, long price, long quantity) {
        return new OrderRequest(participant, clientOrderId, side, OrderType.IOC, quantity, price);
    }

    public static OrderRequest fok(String participant, String clientOrderId, Side side, long price, long quantity) {
        return new OrderRequest(participant, clientOrderId, side, OrderType.FOK, quantity, price);
    }

    public static OrderRequest postOnly(String participant, String clientOrderId, Side side, long price, long quantity) {
        return new OrderRequest(participant, clientOrderId, side, OrderType.POST_ONLY, quantity, price);
    }

    public static OrderRequest market(String participant, String clientOrderId, Side side, long quantity) {
        return new OrderRequest(participant, clientOrderId, side, OrderType.MARKET, quantity, Order.NO_PRICE);
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

    public long getQuantity() {
        return quantity;
    }

    public OptionalLong getPrice() {
        return orderType == OrderType.MARKET ? OptionalLong.empty() : OptionalLong.of(price);
    }

    // For Order only: the raw field, sentinel included.
    long rawPrice() {
        return price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OrderRequest r)) {
            return false;
        }
        return quantity == r.quantity && price == r.price && participant.equals(r.participant)
                && clientOrderId.equals(r.clientOrderId) && side == r.side && orderType == r.orderType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(participant, clientOrderId, side, orderType, quantity, price);
    }

    @Override
    public String toString() {
        return orderType + " " + side + " " + quantity + (orderType == OrderType.MARKET ? "" : " @ " + price)
                + " [" + participant + "/" + clientOrderId + "]";
    }
}
