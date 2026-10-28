package exchangeserver.command;

import exchangeserver.domain.Rules;

import java.time.Instant;
import java.util.Objects;

// Cancels every resting order of one participant (cancel-on-disconnect, D72). An input like any other: journalled
// before it is processed, replayed on restart.
public record CancelAllCommand(long sequenceNumber, Instant timestamp, String participant) implements Command {

    public CancelAllCommand {
        Objects.requireNonNull(timestamp, "timestamp");
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Sequence number must be positive");
        }
        if (participant == null || !Rules.PARTICIPANT.matcher(participant).matches()) {
            throw new IllegalArgumentException("Invalid participant: " + participant);
        }
    }
}
