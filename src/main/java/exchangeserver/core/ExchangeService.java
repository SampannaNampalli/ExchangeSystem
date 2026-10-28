package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.domain.OrderRequest;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.Response;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/*

The single-writer thread model (roadmap step 4A).

Exactly one thread, "exchange-writer", owns the Exchange (sequencer, journal, engine, book). Any number of
caller threads hand in requests through one queue and get a CompletableFuture each. Nothing inside the engine
is synchronised: there is no second thread there to synchronise with (Vision 5).

Batching (step 4C, D52): the writer takes whatever is waiting (up to maxBatchSize), journals it with one fsync,
then processes it ("smart batching"). Under light load a batch is one request, so latency equals fsync-per-
append; under load batches grow on their own. lingerNanos > 0 turns it into group commit with a timeout, kept
only so the benchmark can compare the two.

States (threads guide 4.3):

  state      new request         already queued                   resources
  RUNNING    accepted            processed                        open
  STOPPING   refused (STOPPED)   processed (all are before STOP)  open
  STOPPED    refused (STOPPED)   none                             journal closed, thread ended
  HALTED     refused (HALTED)    refused (HALTED)                 journal closed, thread ended

"Refused" and "never lost" are both true because the state check and the enqueue happen under one lock, and
stop() flips the state and enqueues STOP under the same lock: every accepted request is ahead of STOP. Only
the writer thread closes the journal, as its last act, so nothing can be using it.

The queue itself is unbounded; the capacity is enforced under the lock, so stop() never blocks on a full queue.

 */
public final class ExchangeService {

    public enum State {
        RUNNING,
        STOPPING,
        STOPPED,
        HALTED
    }

    public record Settings(int queueCapacity, int maxBatchSize, long lingerNanos, Clock clock, Exchange.Options exchange) {

        public Settings {
            Objects.requireNonNull(clock, "clock");
            Objects.requireNonNull(exchange, "exchange");
            if (queueCapacity <= 0 || maxBatchSize <= 0 || lingerNanos < 0) {
                throw new IllegalArgumentException("Bad settings");
            }
        }

        public static Settings defaults() {
            return new Settings(100_000, 1_000, 0, Clock.systemUTC(), Exchange.Options.defaults());
        }

        public Settings withQueueCapacity(int capacity) {
            return new Settings(capacity, maxBatchSize, lingerNanos, clock, exchange);
        }

        public Settings withMaxBatchSize(int size) {
            return new Settings(queueCapacity, size, lingerNanos, clock, exchange);
        }

        public Settings withLinger(Duration linger) {
            return new Settings(queueCapacity, maxBatchSize, linger.toNanos(), clock, exchange);
        }

        public Settings withClock(Clock c) {
            return new Settings(queueCapacity, maxBatchSize, lingerNanos, c, exchange);
        }

        public Settings withExchangeOptions(Exchange.Options options) {
            return new Settings(queueCapacity, maxBatchSize, lingerNanos, clock, options);
        }
    }

    private static final System.Logger LOG = System.getLogger(ExchangeService.class.getName());

    private final Pending<Void> stopMarker = new Pending<>(null);       // compared by identity
    private final Exchange exchange;
    private final Exchange.Recovery recovery;
    private final OutputListener listener;
    private final Settings settings;
    private final BlockingQueue<Pending<?>> queue = new LinkedBlockingQueue<>();
    private final Object lock = new Object();
    private final Thread writer;
    private volatile State state = State.RUNNING;
    private volatile Throwable haltCause;

    // Written only by the writer thread; volatile so other threads can read a recent value. Updated after the
    // batch's futures complete, so a caller holding a result may still see the previous values; after stop()
    // returns true (the writer's join) they are final.
    private volatile long batches;
    private volatile long processed;
    private volatile int largestBatch;

    private ExchangeService(Exchange exchange, OutputListener listener, Settings settings) {
        this.exchange = exchange;
        this.recovery = exchange.recovery();
        this.listener = listener;
        this.settings = settings;
        this.writer = new Thread(this::run, "exchange-writer");
        // Not a daemon: the JVM must not exit with accepted requests unprocessed or the journal open.
        this.writer.setDaemon(false);
    }

    // Opens (and replays) the journal on the calling thread, then starts the writer thread. Thread.start() is
    // the happens-before edge that hands the replayed state to the writer.
    public static ExchangeService start(Path journalFile, OutputListener listener, Settings settings) throws IOException {
        return start(journalFile, listener, settings, CommandInterceptor.NONE);
    }

    static ExchangeService start(Path journalFile, OutputListener listener, Settings settings,
                                 CommandInterceptor interceptor) throws IOException {
        Exchange exchange = Exchange.open(journalFile, settings.clock(), interceptor, settings.exchange());
        ExchangeService service = new ExchangeService(exchange, Objects.requireNonNull(listener, "listener"), settings);
        service.writer.start();
        return service;
    }

    // What recovery did at start (L1). Built on the starting thread before the writer existed; immutable.
    public Exchange.Recovery recovery() {
        return recovery;
    }

    // ---------- Hand-in (any thread) ----------

    // Future variants. The caller gets its own future, completed from the common pool, never the one the writer
    // completes:
    //  - continuations the caller attaches (thenAccept, ...) can block without stalling the exchange;
    //  - cancelling it, or completing it via orTimeout(), cannot make the writer skip a request it has already
    //    journalled (the writer decides from its own state, D61).
    public CompletableFuture<Response> submit(OrderRequest order) {
        return handOff(enqueue(new Pending<>(new Request.Submit(Objects.requireNonNull(order, "order"))), null));
    }

    public CompletableFuture<CancelResult> cancel(String participant, long orderId) {
        return handOff(enqueue(new Pending<>(new Request.Cancel(participant, orderId)), null));
    }

    public CompletableFuture<BookSnapshot> snapshot(int depth) {
        return handOff(enqueue(new Pending<>(new Request.Snapshot(depth)), null));
    }

    public CompletableFuture<List<CancelResult>> cancelAll(String participant) {
        return handOff(enqueue(new Pending<>(new Request.CancelAll(participant)), null));
    }

    public CompletableFuture<ResendResult> resend(String participant, long fromOseq) {
        return handOff(enqueue(new Pending<>(new Request.Resend(participant, fromOseq)), null));
    }

    private static <R> CompletableFuture<R> handOff(CompletableFuture<R> internal) {
        CompletableFuture<R> mine = new CompletableFuture<>();
        internal.whenCompleteAsync((value, failure) -> {
            if (failure != null) {
                mine.completeExceptionally(failure);
            } else {
                mine.complete(value);
            }
        });
        return mine;
    }

    // Callback variants, for the gateway: the callback is attached before the request is queued, so it runs on the
    // writer thread right after that command's OutputListener call (D76), or on the caller's thread if the request
    // is refused. A read's callback runs on the writer before any later command's output, which is how SUBSCRIBE
    // and LOGIN join a stream with no gap. The callback must not block: it holds up the whole exchange.
    public void submit(OrderRequest order, BiConsumer<? super Response, ? super Throwable> onDone) {
        enqueue(new Pending<>(new Request.Submit(Objects.requireNonNull(order, "order"))), onDone);
    }

    public void cancel(String participant, long orderId, BiConsumer<? super CancelResult, ? super Throwable> onDone) {
        enqueue(new Pending<>(new Request.Cancel(participant, orderId)), onDone);
    }

    public void snapshot(int depth, BiConsumer<? super BookSnapshot, ? super Throwable> onDone) {
        enqueue(new Pending<>(new Request.Snapshot(depth)), onDone);
    }

    public void cancelAll(String participant, BiConsumer<? super List<CancelResult>, ? super Throwable> onDone) {
        enqueue(new Pending<>(new Request.CancelAll(participant)), onDone);
    }

    public void attach(String participant, BiConsumer<? super Long, ? super Throwable> onDone) {
        enqueue(new Pending<>(new Request.Attach(participant)), onDone);
    }

    public void resend(String participant, long fromOseq, BiConsumer<? super ResendResult, ? super Throwable> onDone) {
        enqueue(new Pending<>(new Request.Resend(participant, fromOseq)), onDone);
    }

    // Returns the internal future: for handOff() only, never to a caller.
    private <R> CompletableFuture<R> enqueue(Pending<R> pending, BiConsumer<? super R, ? super Throwable> onDone) {
        if (onDone != null) {
            pending.result.whenComplete((value, failure) -> {
                // whenComplete would capture a throw in a future nobody reads: log it instead (M9).
                try {
                    onDone.accept(value, failure);
                } catch (RuntimeException | Error callbackBug) {
                    LOG.log(Level.ERROR, "Request callback failed", callbackBug);
                }
            });
        }
        ExchangeException refusal = null;
        synchronized (lock) {
            if (state == State.HALTED) {
                refusal = new ExchangeException(ExchangeException.Reason.HALTED, "exchange halted", haltCause);
            } else if (state != State.RUNNING) {
                refusal = new ExchangeException(ExchangeException.Reason.STOPPED, "exchange is stopping");
            } else if (queue.size() >= settings.queueCapacity()) {
                refusal = new ExchangeException(ExchangeException.Reason.BUSY, "queue full");
            } else {
                queue.add(pending);
            }
        }
        if (refusal != null) {
            pending.fail(refusal);          // outside the lock: callbacks must not run while holding it
        }
        return pending.result;
    }

    // ---------- Stop ----------

    // Refuses new requests, lets the writer finish everything already accepted, and waits for it to end.
    // Idempotent: later callers just wait. Returns false if the writer did not end within the timeout.
    // Not from the writer thread (a listener or callback): it would wait for its own end.
    public boolean stop(Duration timeout) throws InterruptedException {
        if (Thread.currentThread() == writer) {
            throw new IllegalStateException("stop() called on the writer thread: it would wait for itself");
        }
        synchronized (lock) {
            if (state == State.RUNNING) {
                state = State.STOPPING;
                queue.add(stopMarker);
            }
        }
        writer.join(Math.max(1, timeout.toMillis()));
        return !writer.isAlive();
    }

    // Waits until the writer thread has ended (after stop(), or after a halt).
    public void awaitTermination() throws InterruptedException {
        writer.join();
    }

    public State state() {
        return state;
    }

    public Throwable haltCause() {
        return haltCause;
    }

    public long batches() {
        return batches;
    }

    public long processed() {
        return processed;
    }

    public int largestBatch() {
        return largestBatch;
    }

    // ---------- The writer thread ----------

    private void run() {
        List<Pending<?>> batch = new ArrayList<>(settings.maxBatchSize());
        Throwable failure = null;
        try {
            boolean stopping = false;
            while (!stopping) {
                batch.clear();
                batch.add(queue.take());
                queue.drainTo(batch, settings.maxBatchSize() - 1);
                if (settings.lingerNanos() > 0) {
                    linger(batch);
                }
                stopping = batch.remove(stopMarker);    // always last: nothing is accepted after it
                if (!batch.isEmpty()) {
                    exchange.process(batch, listener);
                    batches++;
                    processed += batch.size();
                    largestBatch = Math.max(largestBatch, batch.size());
                }
            }
        } catch (Throwable t) {
            failure = t;
        } finally {
            if (failure != null) {
                halt(failure, batch);
            }
            try {
                exchange.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Closing the journal failed", e);
            }
            if (failure == null) {
                synchronized (lock) {
                    state = State.STOPPED;
                }
            }
        }
    }

    // Group commit with a timeout: keep collecting until the batch is full or the linger time is up.
    private void linger(List<Pending<?>> batch) throws InterruptedException {
        long deadline = System.nanoTime() + settings.lingerNanos();
        while (batch.size() < settings.maxBatchSize() && batch.get(batch.size() - 1) != stopMarker) {
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                break;
            }
            Pending<?> next = queue.poll(left, TimeUnit.NANOSECONDS);
            if (next == null) {
                break;
            }
            batch.add(next);
        }
    }

    // Journal failure, interrupt, or a bug outside the engine: nothing more can be processed safely.
    private void halt(Throwable cause, List<Pending<?>> batch) {
        LOG.log(Level.ERROR, "Exchange halted", cause);
        synchronized (lock) {
            haltCause = cause;
            state = State.HALTED;           // from here on enqueue() refuses, so the drain below is final
        }
        ExchangeException refusal = new ExchangeException(ExchangeException.Reason.HALTED, "exchange halted", cause);
        for (Pending<?> p : batch) {
            p.fail(refusal);                // no-op for those already completed
        }
        List<Pending<?>> left = new ArrayList<>();
        queue.drainTo(left);
        for (Pending<?> p : left) {
            if (p != stopMarker) {
                p.fail(refusal);
            }
        }
    }
}
