package exchangeserver.domain;

/*

LIMIT      match what crosses now, rest the remainder.
IOC        match what crosses now, cancel the remainder.
FOK        fill completely now or do nothing.
MARKET     no price; match at any price inside the market collar (Rules), cancel the remainder.
POST_ONLY  rest without matching; rejected if it would cross on arrival.

 */
public enum OrderType {
    LIMIT,
    IOC,
    FOK,
    MARKET,
    POST_ONLY;

    // Only these two ever rest on the book.
    public boolean canRest() {
        return this == LIMIT || this == POST_ONLY;
    }
}
