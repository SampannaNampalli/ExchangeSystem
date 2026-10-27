package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.Sequencer;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.MatchingEngine;
import exchangeserver.engine.OrderOutput;
import exchangeserver.engine.Response;
import exchangeserver.journal.CommandJournal;

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

open() creates everything itself (sequencer, journal, engine) and replays the journal before returning, so
nobody else holds a reference that could bypass the journal (closes the Spec v8 ownership thread).

A batch: stamp every command, append them all with one write and one fsync, then process them in order.
Nothing is processed before its record is durable, so "visible => durable" holds (write-ahead rule).

Poison pills (D51): if the engine throws on a command, the engine may be half-way through it. The command is
quarantined (a durable SKIP record), the engine is rebuilt by replaying everything before it, and the batch
continues. The failing caller gets INTERNAL_ERROR; nobody else notices. A journal failure is different: it
propagates, and the caller (ExchangeService) halts.

Not thread-safe: exactly one thread may call it. ExchangeService provides that thread.

 */
public final class Exchange implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Exchange.class.getName());

    private final CommandJournal journal;
    private final Sequencer sequencer;
    private final CommandInterceptor interceptor;
    private final Set<Long> skipped;
    private final long replayedOnOpen;
    private MatchingEngine engine;          // replaced after a poison pill

    private Exchange(CommandJournal journal, Sequencer sequencer, MatchingEngine engine, Set<Long> skipped,
                     long replayedOnOpen, CommandInterceptor interceptor) {
        this.journal = journal;
        this.sequencer = sequencer;
        this.engine = engine;
        this.skipped = skipped;
        this.replayedOnOpen = replayedOnOpen;
        this.interceptor = interceptor;
    }

    public static Exchange open(Path journalFile, Clock clock) throws IOException {
        return open(journalFile, clock, CommandInterceptor.NONE);
    }

    static Exchange open(Path journalFile, Clock clock, CommandInterceptor interceptor) throws IOException {
        Objects.requireNonNull(clock, "clock");
        CommandJournal journal = CommandJournal.open(journalFile, Rules.VERSION);
        try {
            MatchingEngine engine = new MatchingEngine();
            Replay.Result replay = Replay.run(journal, engine, Long.MAX_VALUE, interceptor);
            Sequencer sequencer = Sequencer.resume(clock, replay.lastSequenceNumber(), replay.lastOrderId(),
                    replay.lastTimestamp());
            return new Exchange(journal, sequencer, engine, new HashSet<>(replay.skipped()), replay.fed(), interceptor);
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
            long[] last = {0};
            Instant[] lastTimestamp = {null};
            boolean[] exists = {false};
            journal.read(c -> {
                last[0] = c.sequenceNumber();
                lastTimestamp[0] = c.timestamp();
                if (c.sequenceNumber() == sequenceNumber && !(c instanceof SkipCommand)) {
                    exists[0] = true;
                }
            });
            if (!exists[0]) {
                throw new IllegalArgumentException("No new-order or cancel record with sequence number " + sequenceNumber);
            }
            Sequencer sequencer = Sequencer.resume(clock, last[0], 0, lastTimestamp[0]);
            journal.append(sequencer.stampSkip(sequenceNumber));
        }
    }

    // Processes one batch in order, completing each request's future. Throws IOException only if the journal
    // failed; the journal is then fail-stopped, and requests not yet completed are the caller's to fail.
    void process(List<Pending<?>> batch, OutputListener listener) throws IOException {
        List<Command> commands = new ArrayList<>(batch.size());
        for (Pending<?> p : batch) {
            try {
                if (p.request instanceof Request.Submit s) {
                    p.command = sequencer.stampNewOrder(s.order());
                } else if (p.request instanceof Request.Cancel c) {
                    p.command = sequencer.stampCancel(c.participant(), c.orderId());
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
            if (p.request instanceof Request.Snapshot s) {
                p.complete(engine.snapshot(s.depth()));
                continue;
            }
            EngineOutput output;
            try {
                interceptor.beforeEngine(p.command);
                output = apply(p.command);
            } catch (RuntimeException bug) {
                quarantine(p.command, bug);
                p.fail(new ExchangeException(ExchangeException.Reason.INTERNAL_ERROR,
                        "command " + p.command.sequenceNumber() + " failed and was quarantined", bug));
                continue;
            }
            p.complete(output instanceof OrderOutput o ? o.response() : ((CancelOutput) output).result());
            try {
                listener.onOutput(output);
            } catch (RuntimeException listenerBug) {
                LOG.log(Level.ERROR, "Output listener failed on command " + output.sequenceNumber(), listenerBug);
            }
        }
    }

    private EngineOutput apply(Command command) {
        if (command instanceof NewOrderCommand n) {
            return engine.onNewOrder(n);
        }
        if (command instanceof CancelOrderCommand c) {
            return engine.onCancel(c);
        }
        throw new IllegalStateException("Not an engine command: " + command);
    }

    private void quarantine(Command poison, RuntimeException cause) throws IOException {
        LOG.log(Level.ERROR, "Engine failed on command " + poison.sequenceNumber() + "; quarantining it", cause);
        journal.append(sequencer.stampSkip(poison.sequenceNumber()));
        skipped.add(poison.sequenceNumber());

        // Everything before the poison, minus earlier quarantined commands, is exactly what clients have seen.
        // One pass: the journal was verified at open and only this thread has appended since, and `skipped` is
        // already the complete skip set.
        MatchingEngine rebuilt = new MatchingEngine();
        Replay.feed(journal, rebuilt, poison.sequenceNumber(), skipped, interceptor);
        engine = rebuilt;
    }

    // ---------- Single-threaded convenience API (tools, tests, the benchmark baseline) ----------

    public Response submit(OrderRequest request) throws IOException {
        return runOne(new Request.Submit(Objects.requireNonNull(request, "request")));
    }

    public CancelResult cancel(String participant, long orderId) throws IOException {
        return runOne(new Request.Cancel(participant, orderId));
    }

    public BookSnapshot snapshot(int depth) throws IOException {
        return runOne(new Request.Snapshot(depth));
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

    public long replayedOnOpen() {
        return replayedOnOpen;
    }

    public long truncatedBytesOnOpen() {
        return journal.getTruncatedBytesOnOpen();
    }

    public Set<Long> quarantined() {
        return Set.copyOf(skipped);
    }

    // For tests in this package, on the owning thread only.
    MatchingEngine engine() {
        return engine;
    }

    @Override
    public void close() throws IOException {
        journal.close();
    }
}
