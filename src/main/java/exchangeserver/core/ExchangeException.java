package exchangeserver.core;

/*

Why a request got no business result. The caller can tell them apart and act:

BUSY            the queue is full (backpressure); nothing was journalled. Retry later.
STOPPED         the exchange is stopping or stopped; nothing was journalled.
HALTED          the exchange hit a failure it cannot continue from (journal I/O, or the writer thread died).
                The request may or may not be in the journal: after restart, resend with the same client
                order id and the duplicate check tells you (D49).
INTERNAL_ERROR  the engine failed on this command (a bug). The command was quarantined and the exchange
                carried on without it (D51).

 */
public final class ExchangeException extends RuntimeException {

    public enum Reason {
        BUSY,
        STOPPED,
        HALTED,
        INTERNAL_ERROR
    }

    private static final long serialVersionUID = 1L;

    private final Reason reason;

    public ExchangeException(Reason reason, String message) {
        super(reason + ": " + message);
        this.reason = reason;
    }

    public ExchangeException(Reason reason, String message, Throwable cause) {
        super(reason + ": " + message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }
}
