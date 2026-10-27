package exchangeserver.engine;

/*

Why a new order did not simply trade and rest. Decided by the engine after the command is journalled (D13), so
replay reproduces it. Stateless rejects never get this far: OrderRequest refuses them at the gateway.

Every reason except SELF_TRADE_PREVENTED goes with REJECTED (nothing happened); SELF_TRADE_PREVENTED goes with
CANCELLED (the order may have traded with others first, D67). Response enforces the pairing.

 */
public enum Reason {
    // The participant already used this client order id; the response carries the original order id (D49).
    DUPLICATE_CLIENT_ORDER_ID,
    // Priced more than Rules.PRICE_BAND_PERCENT away from the last trade (D50).
    PRICE_OUTSIDE_BAND,
    // The participant already has Rules.MAX_OPEN_ORDERS_PER_PARTICIPANT resting orders, and this one would rest.
    MAX_OPEN_ORDERS,
    // The book already holds Rules.MAX_RESTING_ORDERS orders, and this one would rest (D68).
    BOOK_FULL,
    // A post-only order that would have matched on arrival (D29).
    POST_ONLY_WOULD_CROSS,
    // The next resting order to trade with belongs to the same participant: the incoming remainder is cancelled (D67).
    SELF_TRADE_PREVENTED;

    public boolean isReject() {
        return this != SELF_TRADE_PREVENTED;
    }
}
