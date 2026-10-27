package exchangeserver.engine;

import exchangeserver.domain.Side;

/*

A fill on a resting (passive) order, addressed to its owner. The aggressor learns about its own fills from its
Response; the resting side has no other way to hear about them (closes the Spec v8 "RESTED snapshot" thread).

 */
public record Execution(String participant, long orderId, String clientOrderId, Side side,
                        long price, long quantity, long leavesQuantity) {
}
