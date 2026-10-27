package exchangeserver.engine;

import exchangeserver.domain.Side;

import java.time.Instant;
import java.util.Objects;

/*

price      always the resting level's price.
timestamp  the incoming command's timestamp: the incoming order caused the trade, and reading the clock here
           would make replay produce different trades (D34).

 */
public record Trade(long price, long quantity, long buyOrderId, long sellOrderId, Side aggressorSide, Instant timestamp) {

    public Trade {
        Objects.requireNonNull(aggressorSide, "aggressorSide");
        Objects.requireNonNull(timestamp, "timestamp");
        if (price <= 0) {
            throw new IllegalArgumentException("Price must be positive");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("Quantity must be positive");
        }
    }
}
