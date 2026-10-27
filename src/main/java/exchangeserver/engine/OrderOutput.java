package exchangeserver.engine;

import exchangeserver.book.LevelUpdate;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record OrderOutput(long sequenceNumber, Instant timestamp, Response response,
                          List<Execution> passiveFills, List<LevelUpdate> levelUpdates) implements EngineOutput {

    public OrderOutput {
        Objects.requireNonNull(response, "response");
        passiveFills = List.copyOf(passiveFills);
        levelUpdates = List.copyOf(levelUpdates);
    }

    @Override
    public List<Trade> trades() {
        return response.trades();
    }
}
