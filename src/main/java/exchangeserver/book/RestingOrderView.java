package exchangeserver.book;

import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;

import java.time.Instant;

/*

An immutable copy of one resting order, for reading the book without holding the Order inside it (like Fill).
Used for the engine's fillability walk, cancel-all and snapshots (D67, D71, D72).

 */
public record RestingOrderView(long orderId, String participant, String clientOrderId, Side side, OrderType orderType,
                               long price, long originalQuantity, long filledQuantity, Instant timestamp) {

    public long remainingQuantity() {
        return originalQuantity - filledQuantity;
    }
}
