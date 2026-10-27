package exchangeserver.book;

/*

UNKNOWN_ORDER: not resting (never existed, already filled, already cancelled) or owned by someone else.
Not owning the order is deliberately not a separate status (D48).

 */
public enum CancelStatus {
    CANCELLED,
    UNKNOWN_ORDER
}
