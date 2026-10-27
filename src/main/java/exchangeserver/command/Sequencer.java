package exchangeserver.command;

import exchangeserver.domain.OrderRequest;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/*

The only component that reads the clock or counts (D32). Everything downstream is deterministic.

Two counters, both starting at 1: sequence numbers (every command) and order ids (new orders only).
Timestamps never go backwards: if the clock steps back, the previous timestamp is reused (D33).

Not thread-safe: called only from the writer thread, which is what makes "sequence order == queue order"
true (step 4).

 */
public final class Sequencer {

    private final Clock clock;
    private long nextSequenceNumber;
    private long nextOrderId;
    private Instant previousTimestamp;

    public Sequencer(Clock clock) {
        this(clock, 1, 1, Instant.MIN);
    }

    private Sequencer(Clock clock, long nextSequenceNumber, long nextOrderId, Instant previousTimestamp) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.nextSequenceNumber = nextSequenceNumber;
        this.nextOrderId = nextOrderId;
        this.previousTimestamp = Objects.requireNonNull(previousTimestamp, "previousTimestamp");
    }

    // Continues after a replayed journal (D41). The last new-order id, not the last record's id field: the
    // last record may be a cancel, whose id is a target, not an issued id.
    public static Sequencer resume(Clock clock, long lastSequenceNumber, long lastOrderId, Instant lastTimestamp) {
        if (lastSequenceNumber < 0 || lastOrderId < 0) {
            throw new IllegalArgumentException("Counters cannot be negative");
        }
        return new Sequencer(clock, lastSequenceNumber + 1, lastOrderId + 1,
                lastTimestamp == null ? Instant.MIN : lastTimestamp);
    }

    public NewOrderCommand stampNewOrder(OrderRequest request) {
        Objects.requireNonNull(request, "request");     // before stamping: a null must not use up numbers
        Instant timestamp = nextTimestamp();
        return new NewOrderCommand(nextSequenceNumber++, timestamp, nextOrderId++, request);
    }

    public CancelOrderCommand stampCancel(String participant, long orderId) {
        CancelOrderCommand command = new CancelOrderCommand(nextSequenceNumber, nextTimestamp(), participant, orderId);
        nextSequenceNumber++;                           // only once the command is valid
        return command;
    }

    public CancelAllCommand stampCancelAll(String participant) {
        CancelAllCommand command = new CancelAllCommand(nextSequenceNumber, nextTimestamp(), participant);
        nextSequenceNumber++;
        return command;
    }

    public CheckCommand stampCheck(String digest) {
        CheckCommand command = new CheckCommand(nextSequenceNumber, nextTimestamp(), digest);
        nextSequenceNumber++;
        return command;
    }

    public SkipCommand stampSkip(long skippedSequenceNumber) {
        SkipCommand command = new SkipCommand(nextSequenceNumber, nextTimestamp(), skippedSequenceNumber);
        nextSequenceNumber++;
        return command;
    }

    public long nextSequenceNumber() {
        return nextSequenceNumber;
    }

    public long nextOrderId() {
        return nextOrderId;
    }

    private Instant nextTimestamp() {
        Instant now = clock.instant();
        if (now.isBefore(previousTimestamp)) {
            now = previousTimestamp;
        }
        previousTimestamp = now;
        return now;
    }
}
