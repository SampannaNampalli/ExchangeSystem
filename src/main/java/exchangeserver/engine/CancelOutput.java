package exchangeserver.engine;

import exchangeserver.book.CancelResult;
import exchangeserver.book.LevelUpdate;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record CancelOutput(long sequenceNumber, Instant timestamp, CancelResult result,
                           List<LevelUpdate> levelUpdates) implements EngineOutput {

    public CancelOutput {
        Objects.requireNonNull(result, "result");
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
