package exchangeserver.engine;

import exchangeserver.book.LevelUpdate;

import java.time.Instant;
import java.util.List;

/*

Everything one command produced. The private part goes to the participants involved (the reply, passive
fills); the public part is market data (trades, level updates).

 */
public sealed interface EngineOutput permits OrderOutput, CancelOutput, CancelAllOutput {

    long sequenceNumber();

    Instant timestamp();

    List<Trade> trades();

    List<Execution> passiveFills();

    List<LevelUpdate> levelUpdates();
}
