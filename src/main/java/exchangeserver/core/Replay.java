package exchangeserver.core;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.engine.MatchingEngine;
import exchangeserver.journal.CommandJournal;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/*

Rebuilds engine state by feeding journalled commands through the same engine (D12, D41). Outputs are discarded:
they were delivered in the live run.

Two passes over the file, so memory does not grow with the journal:
  1. Verify the history (D42): sequence numbers exactly 1, 2, 3, ...; timestamps never decrease. Collect the
     skipped (quarantined) sequence numbers and the counters the sequencer resumes from.
  2. Feed every NEW / CANCEL below `stopBefore` that is not skipped.

The skip set must be complete before pass 2 starts: a SKIP record comes after the command it skips.

 */
final class Replay {

    record Result(long lastSequenceNumber, long lastOrderId, Instant lastTimestamp, long fed, Set<Long> skipped) {
    }

    private Replay() {
    }

    static Result run(CommandJournal journal, MatchingEngine engine, long stopBefore, CommandInterceptor interceptor)
            throws IOException {
        Verifier verifier = new Verifier();
        journal.read(verifier);
        long fed = feed(journal, engine, stopBefore, verifier.skipped, interceptor);
        return new Result(verifier.lastSequenceNumber, verifier.lastOrderId, verifier.lastTimestamp, fed,
                Set.copyOf(verifier.skipped));
    }

    // Pass 2 alone, for a journal already verified and a skip set already known (the poison-pill rebuild).
    // Returns how many commands were fed.
    static long feed(CommandJournal journal, MatchingEngine engine, long stopBefore, Set<Long> skipped,
                     CommandInterceptor interceptor) throws IOException {
        long[] fed = {0};
        journal.read(command -> {
            if (command.sequenceNumber() >= stopBefore || command instanceof SkipCommand
                    || skipped.contains(command.sequenceNumber())) {
                return;
            }
            interceptor.beforeEngine(command);
            if (command instanceof NewOrderCommand n) {
                engine.onNewOrder(n);
            } else if (command instanceof CancelOrderCommand c) {
                engine.onCancel(c);
            }
            fed[0]++;
        });
        return fed[0];
    }

    private static final class Verifier implements java.util.function.Consumer<Command> {
        long lastSequenceNumber;
        long lastOrderId;           // from NEW records only, skipped ones included: that id was issued
        Instant lastTimestamp;
        final Set<Long> skipped = new HashSet<>();

        @Override
        public void accept(Command command) {
            if (command.sequenceNumber() != lastSequenceNumber + 1) {
                throw new IllegalStateException("Journal sequence gap or repeat: " + command.sequenceNumber()
                        + " after " + lastSequenceNumber);
            }
            if (lastTimestamp != null && command.timestamp().isBefore(lastTimestamp)) {
                throw new IllegalStateException("Journal timestamp goes backwards at sequence " + command.sequenceNumber());
            }
            lastSequenceNumber = command.sequenceNumber();
            lastTimestamp = command.timestamp();
            if (command instanceof NewOrderCommand n) {
                lastOrderId = n.orderId();
            } else if (command instanceof SkipCommand s) {
                skipped.add(s.skippedSequenceNumber());
            }
        }
    }
}
