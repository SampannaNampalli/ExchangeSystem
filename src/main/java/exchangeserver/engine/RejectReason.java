package exchangeserver.engine;

/*

State-dependent rejects, decided by the engine after the command is journalled (D13), so replay reproduces
them. Stateless rejects never get this far: OrderRequest refuses them at the gateway.

 */
public enum RejectReason {
    // The participant already used this client order id; the response carries the original order id (D49).
    DUPLICATE_CLIENT_ORDER_ID,
    // Priced more than Rules.PRICE_BAND_PERCENT away from the last trade (D50).
    PRICE_OUTSIDE_BAND,
    // The participant already has Rules.MAX_OPEN_ORDERS_PER_PARTICIPANT resting orders.
    MAX_OPEN_ORDERS,
    // A post-only order that would have matched on arrival (D29).
    POST_ONLY_WOULD_CROSS
}
