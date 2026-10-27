package exchangeserver.command;

import exchangeserver.domain.Rules;

import java.time.Instant;
import java.util.Objects;

// orderId is the target; a cancel is not issued an id of its own (D32).
public record CancelOrderCommand(long sequenceNumber, Instant timestamp, String participant, long orderId)
        implements Command {

    public CancelOrderCommand {
        Objects.requireNonNull(timestamp, "timestamp");
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Sequence number must be positive");
        }
        if (participant == null || !Rules.PARTICIPANT.matcher(participant).matches()) {
            throw new IllegalArgumentException("Invalid participant: " + participant);
        }
        if (orderId <= 0) {
            throw new IllegalArgumentException("Order id must be positive");
        }
    }
}
