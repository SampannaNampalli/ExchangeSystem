package exchangeserver.engine;

import exchangeserver.book.RestingOrderView;

import java.util.List;

/*

Everything the engine needs to continue after a snapshot (D71): the counters it checks, the price band reference,
and the resting orders in book priority order. The client-order-id map is not stored: after a snapshot it holds
only the resting orders' ids (a client order id is unique per participant within one journal, D71 revises D49).

 */
public record EngineState(long lastSequenceNumber, long lastOrderId, long lastTradePrice, List<RestingOrderView> orders) {

    public EngineState {
        if (lastSequenceNumber < 0 || lastOrderId < 0 || lastTradePrice < 0) {
            throw new IllegalArgumentException("Counters and prices cannot be negative");
        }
        orders = List.copyOf(orders);
    }
}
