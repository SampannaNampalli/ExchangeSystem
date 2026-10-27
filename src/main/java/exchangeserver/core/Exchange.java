package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.Sequencer;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.CancelAllOutput;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.MatchingEngine;
import exchangeserver.engine.OrderOutput;
import exchangeserver.engine.Response;
import exchangeserver.journal.CommandJournal;
import exchangeserver.journal.Durable;
import exchangeserver.journal.JournalLock;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;

/*

The one live path: stamp -> write ahead -> process (D40), one batch at a time.

open() creates everything itself (sequencer, journal, ledger) and replays the journal before returning, so
nobody else holds a reference that could bypass the journal (closes the Spec v8 ownership thread).

A batch: stamp every command, append them all with one write and one fsync, then process them in order.
Nothing is processed before its record is durable, so "visible => durable" holds (write-ahead rule). For each
command the listener hears the outputs first (market data and the numbered private events, D73), then the
request's future completes (D76).

Every `checkInterval` commands a CHECK record carries the digest of all outputs so far (D70), so replay can
prove it rebuilt the same history.

Poison pills (D51): if the engine throws on a command, the engine may be half-way through it. The command is
quarantined (a durable SKIP record), the ledger is rebuilt by replaying everything before it, and the batch
continues. The failing caller gets INTERNAL_ERROR; nobody else notices. A journal failure is different: it
propagates, and the caller (ExchangeService) halts.

Not thread-safe: exactly one thread may call it. ExchangeService provides that thread.

 */
public final class Exchange implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Exchange.class.getName());

    // checkInterval: commands between CHECK records (D70). outputRetention: private events kept per participant
    // for RESEND (D73).
    public record Options(int checkInterval, int outputRetention) {

        public Options {
            if (checkInterval <= 0 || outputRetention <= 0) {
                throw new IllegalArgumentException("Bad options");
            }
        }

        public static Options defaults() {
            return new Options(10_000, 1_000);
        }
    }

    // What recovery did (L1), for the operator.
    public record Recovery(long baseSequence, long lastSequence, long replayed, long truncatedBytes, Set<Long> quarantined,
                           int adoptedRulesFrom) {
    }

    // What compaction did (D71).
    public record Compaction(long lastSequence, int restingOrders, Path snapshot, Path archive) {
    }

    private final CommandJournal journal;
    private final Path journalFile;
    private final Sequencer sequencer;
    private final CommandInterceptor interceptor;
    private final Options options;
    private final Set<Long> skipped;
    private final Recovery recovery;
    private Ledger ledger;                  // replaced after a poison pill
    private long commandsSinceCheck;

    private Exchange(CommandJournal journal, Path journalFile, Sequencer sequencer, Ledger ledger, Set<Long> skipped,
                     Recovery recovery, CommandInterceptor interceptor, Options options) {
        this.journal = journal;
        this.journalFile = journalFile;
        this.sequencer = sequencer;
        this.ledger = ledger;
        this.skipped = skipped;
        this.recovery = recovery;
        this.interceptor = interceptor;
        this.options = options;
    }

    public static Exchange open(Path journalFile, Clock clock) throws IOException {
        return open(journalFile, clock, CommandInterceptor.NONE, Options.defaults());
    }

    static Exchange open(Path journalFile, Clock clock, CommandInterceptor interceptor) throws IOException {
        return open(journalFile, clock, interceptor, Options.defaults());
    }

    static Exchange open(Path journalFile, Clock clock, CommandInterceptor interceptor, Options options) throws IOException {
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(options, "options");
        CommandJournal journal = CommandJournal.open(journalFile, Rules.VERSION);
        try {
            Replay.Result replay = Replay.run(journal, journalFile, Long.MAX_VALUE, interceptor, options.outputRetention());
            Sequencer sequencer = Sequencer.resume(clock, replay.lastSequenceNumber(), replay.lastOrderId(),
                    replay.lastTimestamp());
            Recovery recovery = new Recovery(replay.baseSequence(), replay.lastSequenceNumber(), replay.fed(),
                    journal.getTruncatedBytesOnOpen(), replay.skipped(), journal.adoptedRulesFrom());
            return new Exchange(journal, journalFile, sequencer, replay.ledger(), new HashSet<>(replay.skipped()), recovery,
                    interceptor, options);
        } catch (IOException | RuntimeException | Error e) {
            // The exchange owns the journal from here (D43): a failed open must not leak the file handle.
            try {
                journal.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    // Operator tool for a poison pill found at startup (D51): appends a SKIP for that command without replaying
    // through the engine, so the next open() skips it. Only for a command the operator has inspected.
    public static void quarantine(Path journalFile, long sequenceNumber, Clock clock) throws IOException {
        try (CommandJournal journal = CommandJournal.open(journalFile, Rules.VERSION)) {
            long[] last = {journal.baseSequence()};
            Instant[] lastTimestamp = {null};
            boolean[] exists = {false};
            Set<Long> alreadySkipped = new HashSet<>();
            journal.read(c -> {
                last[0] = c.sequenceNumber();
                lastTimestamp[0] = c.timestamp();
                if (c.sequenceNumber() == sequenceNumber && (c instanceof NewOrderCommand || c instanceof CancelOrderCommand
                        || c instanceof CancelAllCommand)) {
                    exists[0] = true;
                } else if (c instanceof SkipCommand s) {
                    alreadySkipped.add(s.skippedSequenceNumber());
                }
            });
            if (!exists[0]) {
                throw new IllegalArgumentException("No new-order, cancel or cancel-all record with sequence number " + sequenceNumber);
            }
            if (alreadySkipped.contains(sequenceNumber)) {
                throw new IllegalArgumentException("Command " + sequenceNumber + " is already quarantined");       // L3
            }
            Sequencer sequencer = Sequencer.resume(clock, last[0], 0, lastTimestamp[0]);
            journal.append(sequencer.stampSkip(sequenceNumber));
        }
    }

    // Operator tool (D71), run while the exchange is stopped: replays the journal, writes a snapshot of the state
    // after its last command, archives the journal, and replaces it with an empty one that continues from the
    // snapshot. Bounds recovery time and memory; ends the scope of client order ids (only resting orders keep
    // theirs); and is the way across a rules change: compact with the old build, start the new build.
    // Crash-safe at every step: the old journal stays in place until the new one atomically replaces it, and each
    // journal names the snapshot it needs by sequence number. Returns null if there is nothing to compact.
    public static Compaction compact(Path journalFile) throws IOException {
        try (JournalLock lock = JournalLock.acquire(journalFile)) {
            Replay.Result replay;
            try (CommandJournal journal = CommandJournal.open(journalFile, Rules.VERSION, lock)) {
                replay = Replay.run(journal, journalFile, Long.MAX_VALUE, CommandInterceptor.NONE,
                        Options.defaults().outputRetention());
            }
            if (replay.lastSequenceNumber() == replay.baseSequence()) {
                return null;
            }
            long last = replay.lastSequenceNumber();
            Snapshot snapshot = replay.ledger().snapshot(last, replay.lastOrderId(), replay.lastTimestamp());
            Path snapshotFile = Snapshot.path(journalFile, last);
            snapshot.write(snapshotFile);
            Path archive = journalFile.resolveSibling(journalFile.getFileName() + ".archive-"
                    + (replay.baseSequence() + 1) + "-" + last);
            Durable.copyDurably(journalFile, archive);
            CommandJournal.writeFresh(journalFile, Rules.VERSION, last);
            return new Compaction(last, snapshot.orders().size(), snapshotFile, archive);
        }
    }

    // Processes one batch in order, completing each request's future. Throws IOException only if the journal
    // failed; the journal is then fail-stopped, and requests not yet completed are the caller's to fail.
    void process(List<Pending<?>> batch, OutputListener listener) throws IOException {
        List<Command> commands = new ArrayList<>(batch.size() + 1);
        if (commandsSinceCheck >= options.checkInterval()) {
            commands.add(sequencer.stampCheck(ledger.digest()));       // D70: covers every command before it
            commandsSinceCheck = 0;
        }
        for (Pending<?> p : batch) {
            try {
                if (p.request instanceof Request.Submit s) {
                    p.command = sequencer.stampNewOrder(s.order());
                } else if (p.request instanceof Request.Cancel c) {
                    p.command = sequencer.stampCancel(c.participant(), c.orderId());
                } else if (p.request instanceof Request.CancelAll c) {
                    p.command = sequencer.stampCancelAll(c.participant());
                }
            } catch (RuntimeException invalid) {
                p.refused = true;                   // refused before stamping: uses up no number, never journalled
                p.fail(invalid);
                continue;
            }
            if (p.command != null) {
                commands.add(p.command);
            }
        }

        journal.appendAll(commands);                // durable before any effect

        for (Pending<?> p : batch) {
            // Decided from the writer's own state, never from the future: a journalled command must reach the
            // engine whatever happened to anyone's future, or the live book and the replayed one differ (D61).
            if (p.refused) {
                continue;
            }
            if (p.command == null) {
                p.complete(read(p.request));
                continue;
            }
            Ledger.Applied applied;
            try {
                interceptor.beforeEngine(p.command);
                applied = ledger.apply(p.command);
            } catch (RuntimeException bug) {
                quarantine(p.command, bug);
                p.fail(new ExchangeException(ExchangeException.Reason.INTERNAL_ERROR,
                        "command " + p.command.sequenceNumber() + " failed and was quarantined", bug));
                continue;
            }
            commandsSinceCheck++;
            try {
                listener.onOutput(applied.output(), applied.events());
            } catch (RuntimeException listenerBug) {
                LOG.log(Level.ERROR, "Output listener failed on command " + applied.output().sequenceNumber(), listenerBug);
            }
            p.complete(result(applied.output()));   // after the listener: the request's lines went out first (D76)
        }
    }

    private Object read(Request<?> request) {
        if (request instanceof Request.Snapshot s) {
            return ledger.engine().snapshot(s.depth());
        }
        if (request instanceof Request.Attach a) {
            return ledger.log().lastOseq(a.participant());
        }
        if (request instanceof Request.Resend r) {
            return ledger.log().since(r.participant(), r.fromOseq());
        }
        throw new IllegalStateException("Not a read: " + request);
    }

    private static Object result(EngineOutput output) {
        if (output instanceof OrderOutput o) {
            return o.response();
        }
        if (output instanceof CancelOutput c) {
            return c.result();
        }
        return ((CancelAllOutput) output).results();
    }

    private void quarantine(Command poison, RuntimeException cause) throws IOException {
        LOG.log(Level.ERROR, "Engine failed on command " + poison.sequenceNumber() + "; quarantining it", cause);
        journal.append(sequencer.stampSkip(poison.sequenceNumber()));
        skipped.add(poison.sequenceNumber());

        // Everything before the poison, minus earlier quarantined commands, is exactly what clients have seen.
        // One pass from the snapshot: the journal was verified at open and only this thread has appended since,
        // and `skipped` is already the complete skip set.
        ledger = Replay.rebuild(journal, journalFile, poison.sequenceNumber(), skipped, interceptor, options.outputRetention());
    }

    // ---------- Single-threaded convenience API (tools, tests, the benchmark baseline) ----------

    public Response submit(OrderRequest request) throws IOException {
        return runOne(new Request.Submit(Objects.requireNonNull(request, "request")));
    }

    public CancelResult cancel(String participant, long orderId) throws IOException {
        return runOne(new Request.Cancel(participant, orderId));
    }

    public List<CancelResult> cancelAll(String participant) throws IOException {
        return runOne(new Request.CancelAll(participant));
    }

    public BookSnapshot snapshot(int depth) throws IOException {
        return runOne(new Request.Snapshot(depth));
    }

    public ResendResult resend(String participant, long fromOseq) throws IOException {
        return runOne(new Request.Resend(participant, fromOseq));
    }

    private <R> R runOne(Request<R> request) throws IOException {
        Pending<R> p = new Pending<>(request);
        process(List.of(p), OutputListener.NONE);
        try {
            return p.result.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException r) {
                throw r;
            }
            throw e;
        }
    }

    // ---------- State ----------

    public long lastSequenceNumber() {
        return sequencer.nextSequenceNumber() - 1;
    }

    public Recovery recovery() {
        return recovery;
    }

    public long replayedOnOpen() {
        return recovery.replayed();
    }

    public long truncatedBytesOnOpen() {
        return recovery.truncatedBytes();
    }

    public Set<Long> quarantined() {
        return Set.copyOf(skipped);
    }

    // For tests in this package, on the owning thread only.
    MatchingEngine engine() {
        return ledger.engine();
    }

    String digest() {
        return ledger.digest();
    }

    @Override
    public void close() throws IOException {
        journal.close();
    }
}
