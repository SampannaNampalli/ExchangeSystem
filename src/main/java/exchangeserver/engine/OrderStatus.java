package exchangeserver.engine;

// The outcome of one new order (D23). The client's next action differs for each.
public enum OrderStatus {
    FILLED,
    RESTED,
    CANCELLED,
    REJECTED
}
