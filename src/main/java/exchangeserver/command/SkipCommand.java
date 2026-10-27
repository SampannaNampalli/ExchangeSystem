package exchangeserver.command;

import java.time.Instant;
import java.util.Objects;

/*

Quarantines an earlier command (the poison-pill policy, D51): replay does not feed the skipped command to the
engine. Written when the live engine throws on a command, or by an operator (Main --quarantine <seq>).

The skipped command stays in the journal: history is never edited, only annotated.

 */
public record SkipCommand(long sequenceNumber, Instant timestamp, long skippedSequenceNumber) implements Command {

    public SkipCommand {
        Objects.requireNonNull(timestamp, "timestamp");
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Sequence number must be positive");
        }
        if (skippedSequenceNumber <= 0 || skippedSequenceNumber >= sequenceNumber) {
            throw new IllegalArgumentException("Can only skip an earlier command, not " + skippedSequenceNumber);
        }
    }
}
