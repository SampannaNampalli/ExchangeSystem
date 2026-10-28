package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.domain.OrderRequest;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.Response;

import java.util.List;

/*

What a caller hands in, before the writer thread stamps it. R is the type of its result.

The queue carries these, not stamped commands: stamping happens on the writer thread, so sequence order is
exactly processing order and no producer reads the clock (step 4A).

Submit, Cancel and CancelAll become journalled commands. Snapshot, Attach and Resend are reads: they go through
the queue like everything else, so each sees the state after every earlier request and before every later one,
and they are never journalled.

 */
sealed interface Request<R> permits Request.Submit, Request.Cancel, Request.CancelAll, Request.Snapshot,
        Request.Attach, Request.Resend {

    record Submit(OrderRequest order) implements Request<Response> {
    }

    record Cancel(String participant, long orderId) implements Request<CancelResult> {
    }

    // D72: the results of the cancels it made, in book priority order.
    record CancelAll(String participant) implements Request<List<CancelResult>> {
    }

    record Snapshot(int depth) implements Request<BookSnapshot> {
        public Snapshot {
            // Checked here, on the caller's thread: a bad read must not reach (and halt) the writer thread.
            if (depth <= 0) {
                throw new IllegalArgumentException("Depth must be positive");
            }
        }
    }

    // D73: the participant's latest output sequence number. The callback runs on the writer thread before any later
    // output, which is how a session joins the participant's stream with no gap.
    record Attach(String participant) implements Request<Long> {
    }

    // D73: the participant's retained private events from `fromOseq` on.
    record Resend(String participant, long fromOseq) implements Request<ResendResult> {
        public Resend {
            if (fromOseq <= 0) {
                throw new IllegalArgumentException("Output sequence numbers start at 1");
            }
        }
    }
}
