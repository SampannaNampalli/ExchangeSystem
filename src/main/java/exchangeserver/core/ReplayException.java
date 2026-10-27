package exchangeserver.core;

/*

Replay could not rebuild the state, at a known record (L2: so the operator gets the right advice).

ENGINE_FAILURE  the engine threw on this command: a poison pill. Inspect it; `--quarantine <seq>` skips it.
DIVERGED        a CHECK record's digest differs from what replay produced (D70): this build processes the old
                commands differently. Start from a snapshot written by the build that wrote the journal.

 */
public final class ReplayException extends IllegalStateException {

    public enum Kind {
        ENGINE_FAILURE,
        DIVERGED
    }

    private static final long serialVersionUID = 1L;

    private final Kind kind;
    private final long sequenceNumber;

    ReplayException(Kind kind, long sequenceNumber, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
        this.sequenceNumber = sequenceNumber;
    }

    public Kind kind() {
        return kind;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }
}
