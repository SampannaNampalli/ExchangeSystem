package exchangeserver.core;

import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.MatchingEngine;

import java.time.Instant;
import java.util.List;

/*

Everything derived from the commands, kept together so it is rebuilt together: the engine (book and counters), the
output digest (D70) and the per-participant output streams (D73). A poison-pill rebuild replaces the whole ledger,
so none of the three can disagree with the others.

Not thread-safe: owned by the writer thread.

 */
final class Ledger {

    record Applied(EngineOutput output, List<PrivateEvent> events) {
    }

    private final MatchingEngine engine;
    private final OutputDigest digest;
    private final OutputLog log;

    private Ledger(MatchingEngine engine, OutputDigest digest, OutputLog log) {
        this.engine = engine;
        this.digest = digest;
        this.log = log;
    }

    static Ledger from(Snapshot snapshot, int retention) {
        MatchingEngine engine = snapshot.lastSequence() == 0 ? new MatchingEngine() : MatchingEngine.restore(snapshot.engineState());
        return new Ledger(engine, OutputDigest.resume(snapshot.digest()), OutputLog.resume(retention, snapshot.lastOseqs()));
    }

    Applied apply(Command command) {
        EngineOutput output;
        if (command instanceof NewOrderCommand n) {
            output = engine.onNewOrder(n);
        } else if (command instanceof CancelOrderCommand c) {
            output = engine.onCancel(c);
        } else if (command instanceof CancelAllCommand c) {
            output = engine.onCancelAll(c);
        } else {
            throw new IllegalStateException("Not an engine command: " + command);
        }
        digest.update(output);
        return new Applied(output, log.record(command, output));
    }

    Snapshot snapshot(long lastSequence, long lastOrderId, Instant lastTimestamp) {
        var state = engine.exportState();
        return new Snapshot(lastSequence, Math.max(lastOrderId, state.lastOrderId()), lastTimestamp, state.lastTradePrice(),
                digest.hex(), state.orders(), log.lastOseqs());
    }

    String digest() {
        return digest.hex();
    }

    MatchingEngine engine() {
        return engine;
    }

    OutputLog log() {
        return log;
    }
}
