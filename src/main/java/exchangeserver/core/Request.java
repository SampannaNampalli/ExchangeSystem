package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.domain.OrderRequest;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.Response;

/*

What a caller hands in, before the writer thread stamps it. R is the type of its result.

The queue carries these, not stamped commands: stamping happens on the writer thread, so sequence order is
exactly processing order and no producer reads the clock (step 4A).

 */
sealed interface Request<R> permits Request.Submit, Request.Cancel, Request.Snapshot {

    record Submit(OrderRequest order) implements Request<Response> {
    }

    record Cancel(String participant, long orderId) implements Request<CancelResult> {
    }

    // A read. Goes through the queue like everything else, so it sees the state after every earlier request
    // and before every later one. Never journalled.
    record Snapshot(int depth) implements Request<BookSnapshot> {
        public Snapshot {
            // Checked here, on the caller's thread: a bad read must not reach (and halt) the writer thread.
            if (depth <= 0) {
                throw new IllegalArgumentException("Depth must be positive");
            }
        }
    }
}
