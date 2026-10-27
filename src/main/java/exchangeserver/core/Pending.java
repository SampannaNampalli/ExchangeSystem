package exchangeserver.core;

import exchangeserver.command.Command;

import java.util.concurrent.CompletableFuture;

/*

A request in flight, with its own result slot. One future per request, never shared: a shared slot hands every
waiter the first result (threads guide, Lab 6).

No field needs volatile: the queue's put -> take edge publishes the request to the writer thread, and the
future's complete -> get edge publishes the result back.

 */
final class Pending<R> {

    final Request<R> request;
    final CompletableFuture<R> result = new CompletableFuture<>();
    Command command;                // set by the writer thread when stamped; null for reads
    boolean refused;                // set by the writer thread when stamping refused it; never journalled

    Pending(Request<R> request) {
        this.request = request;
    }

    @SuppressWarnings("unchecked")
    void complete(Object value) {
        ((CompletableFuture<Object>) result).complete(value);
    }

    // A no-op if the future is already done, so failing a whole batch after a partial success is safe.
    void fail(Throwable cause) {
        result.completeExceptionally(cause);
    }
}
