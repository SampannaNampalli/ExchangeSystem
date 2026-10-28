package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.domain.Side;
import exchangeserver.engine.Execution;
import exchangeserver.engine.Response;

/*

One message of a participant's private stream (D73): the outcome of its own requests and the fills on its resting
orders. Each participant's events are numbered 1, 2, 3, ... (oseq). The numbering is derived from the commands
alone, so replay rebuilds it exactly: after a restart a client can ask for everything after the last oseq it saw.

 */
public sealed interface PrivateEvent permits PrivateEvent.OrderReply, PrivateEvent.CancelReply, PrivateEvent.MakerFill {

    String participant();

    long oseq();

    // The outcome of a new order, for its submitter. side and quantity are the request's (for the taker FILL lines).
    record OrderReply(String participant, long oseq, Side side, long quantity, Response response) implements PrivateEvent {
    }

    // The outcome of a cancel; byCancelAll marks one of the cancels a CANCEL_ALL made (D72).
    record CancelReply(String participant, long oseq, CancelResult result, boolean byCancelAll) implements PrivateEvent {
    }

    // A fill on the participant's resting order.
    record MakerFill(String participant, long oseq, Execution execution) implements PrivateEvent {
    }
}
