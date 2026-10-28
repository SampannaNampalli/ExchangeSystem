package exchangeserver.engine;

import exchangeserver.book.CancelResult;
import exchangeserver.book.LevelUpdate;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

// What a CANCEL_ALL did (D72): one CANCELLED result per order it removed, in book priority order.
public record CancelAllOutput(long sequenceNumber, Instant timestamp, String participant, List<CancelResult> results,
                              List<LevelUpdate> levelUpdates) implements EngineOutput {

    public CancelAllOutput {
        Objects.requireNonNull(participant, "participant");
        results = List.copyOf(results);
        levelUpdates = List.copyOf(levelUpdates);
    }

    @Override
    public List<Trade> trades() {
        return List.of();
    }

    @Override
    public List<Execution> passiveFills() {
        return List.of();
    }
}
