package exchangeserver.engine;

import java.util.List;
import java.util.Objects;

/*

What the submitter of a new order is told (D22). The fields cannot contradict each other: the constructor
checks the invariants, so a bug fails here instead of reaching a client.

orderId    the id to cancel with. For DUPLICATE_CLIENT_ORDER_ID it is the original order's id (D49).
trades     the aggressor's own fills, in match order.

 */
public record Response(long orderId, String clientOrderId, OrderStatus status, Reason reason,
                       long filledQuantity, long remainingQuantity, List<Trade> trades) {

    public Response {
        Objects.requireNonNull(clientOrderId, "clientOrderId");
        Objects.requireNonNull(status, "status");
        trades = List.copyOf(trades);

        long traded = 0;
        for (Trade t : trades) {
            traded += t.quantity();
        }
        if (traded != filledQuantity) {
            throw new IllegalStateException("Trades add up to " + traded + ", filled is " + filledQuantity);
        }
        if (remainingQuantity < 0) {
            throw new IllegalStateException("Negative remaining quantity");
        }
        if ((status == OrderStatus.REJECTED) != (reason != null && reason.isReject())) {
            throw new IllegalStateException("A reject reason goes with REJECTED and nothing else");
        }
        if (reason == Reason.SELF_TRADE_PREVENTED && status != OrderStatus.CANCELLED) {
            throw new IllegalStateException("SELF_TRADE_PREVENTED goes with CANCELLED only");
        }
        switch (status) {
            case FILLED -> {
                if (remainingQuantity != 0) {
                    throw new IllegalStateException("FILLED with " + remainingQuantity + " remaining");
                }
            }
            case RESTED, CANCELLED -> {
                if (remainingQuantity == 0) {
                    throw new IllegalStateException(status + " with nothing remaining");
                }
            }
            case REJECTED -> {
                if (filledQuantity != 0) {
                    throw new IllegalStateException("REJECTED after trading");
                }
            }
        }
    }
}
