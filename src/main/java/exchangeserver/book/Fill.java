package exchangeserver.book;

import exchangeserver.domain.Side;

/*

What OrderBook.fillBest() did to one resting order. An immutable copy, so the engine learns about the resting
order without ever holding the Order object that sits in the book.

 */
public record Fill(long restingOrderId, String participant, String clientOrderId, Side restingSide,
                   long price, long quantity, long leavesQuantity) {
}
