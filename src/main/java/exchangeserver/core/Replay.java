package exchangeserver.core;

import exchangeserver.command.CheckCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.journal.CommandJournal;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/*

Rebuilds the ledger (engine, output digest, output streams) by feeding journalled commands through the same
engine (D12, D41), starting from the snapshot the journal's header names (D71). Outputs are not delivered: they
were delivered in the live run.

Two passes over the file, so memory does not grow with the journal:
  1. Verify the history (D42): sequence numbers exactly base+1, base+2, ...; timestamps never decrease. Collect
     the skipped (quarantined) sequence numbers and the counters the sequencer resumes from.
  2. Feed every NEW / CANCEL / CANCEL_ALL below `stopBefore` that is not skipped, and compare every CHECK
     record's digest with the replayed one (D70).

The skip set must be complete before pass 2 starts: a SKIP record comes after the command it skips.

 */
final class Replay {

    record Result(Ledger ledger, long lastSequenceNumber, long lastOrderId, Instant lastTimestamp, long fed,
                  Set<Long> skipped, long baseSequence) {
    }

    private Replay() {
    }

    static Result run(CommandJournal journal, Path journalFile, long stopBefore, CommandInterceptor interceptor,
                      int retention) throws IOException {
        Snapshot snapshot = snapshotFor(journal, journalFile);
        Verifier verifier = new Verifier(snapshot);
        journal.read(verifier);
        Ledger ledger = Ledger.from(snapshot, retention);
        long fed = feed(journal, ledger, stopBefore, verifier.skipped, interceptor);
        return new Result(ledger, verifier.lastSequenceNumber, verifier.lastOrderId, verifier.lastTimestamp, fed,
                Set.copyOf(verifier.skipped), journal.baseSequence());
    }

    // Pass 2 from the snapshot, for a journal already verified and a skip set already known (the poison-pill
    // rebuild).
    static Ledger rebuild(CommandJournal journal, Path journalFile, long stopBefore, Set<Long> skipped,
                          CommandInterceptor interceptor, int retention) throws IOException {
        Ledger ledger = Ledger.from(snapshotFor(journal, journalFile), retention);
        feed(journal, ledger, stopBefore, skipped, interceptor);
        return ledger;
    }

    static Snapshot snapshotFor(CommandJournal journal, Path journalFile) throws IOException {
        long base = journal.baseSequence();
        if (base == 0) {
            return Snapshot.empty();
        }
        Snapshot snapshot = Snapshot.read(Snapshot.path(journalFile, base));
        if (snapshot.lastSequence() != base) {
            throw new IllegalStateException("Snapshot for base " + base + " holds sequence " + snapshot.lastSequence());
        }
        return snapshot;
    }

    // Returns how many commands were fed.
    static long feed(CommandJournal journal, Ledger ledger, long stopBefore, Set<Long> skipped,
                     CommandInterceptor interceptor) throws IOException {
        long[] fed = {0};
        journal.read(command -> {
            long seq = command.sequenceNumber();
            if (seq >= stopBefore || command instanceof SkipCommand || skipped.contains(seq)) {
                return;
            }
            if (command instanceof CheckCommand check) {
                if (!check.digest().equals(ledger.digest())) {
                    throw new ReplayException(ReplayException.Kind.DIVERGED, seq, "Replay diverged before CHECK " + seq
                            + ": the journal recorded output digest " + check.digest() + ", this build produced "
                            + ledger.digest() + ". This build processes the old commands differently; start it from a "
                            + "snapshot written by the build that wrote this journal (--compact).", null);
                }
                return;
            }
            try {
                interceptor.beforeEngine(command);      // the test seam stands in for the engine here
                ledger.apply(command);
            } catch (RuntimeException e) {
                throw new ReplayException(ReplayException.Kind.ENGINE_FAILURE, seq, "Replay failed on command " + seq
                        + " (" + e.getMessage() + ")", e);
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

        Verifier(Snapshot snapshot) {
            lastSequenceNumber = snapshot.lastSequence();
            lastOrderId = snapshot.lastOrderId();
            lastTimestamp = snapshot.lastTimestamp();
        }

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
