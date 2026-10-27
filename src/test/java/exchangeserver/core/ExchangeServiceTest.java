package exchangeserver.core;

import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.OrderStatus;
import exchangeserver.engine.Response;
import exchangeserver.journal.CommandJournal;
import exchangeserver.testing.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// Every wait has a timeout (threads guide 4.4): a bug must fail the test, not hang it.
// The writer's stats (batches, processed, largestBatch) are read only after stop(): the writer updates them after
// a batch's futures complete, so a completed future does not make them current; the writer's join() does.
class ExchangeServiceTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @TempDir
    Path dir;

    private final MutableClock clock = MutableClock.at("2026-01-01T10:00:00Z");
    private final List<ExchangeService> started = new ArrayList<>();

    private Path journal() {
        return dir.resolve("service.journal");
    }

    private ExchangeService start(ExchangeService.Settings settings, CommandInterceptor interceptor, OutputListener listener)
            throws IOException {
        ExchangeService s = ExchangeService.start(journal(), listener, settings.withClock(clock), interceptor);
        started.add(s);
        return s;
    }

    private ExchangeService start(CommandInterceptor interceptor) throws IOException {
        return start(ExchangeService.Settings.defaults(), interceptor, OutputListener.NONE);
    }

    @AfterEach
    void stopEverything() throws InterruptedException {
        for (ExchangeService s : started) {
            assertTrue(s.stop(WAIT), "writer thread did not end");
        }
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(WAIT.toSeconds(), TimeUnit.SECONDS);
    }

    private static ExchangeException.Reason refusal(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(WAIT.toSeconds(), TimeUnit.SECONDS));
        return assertInstanceOf(ExchangeException.class, e.getCause()).getReason();
    }

    private static OrderRequest order(String p, String c, Side side, long price) {
        return OrderRequest.limit(p, c, side, price, 1);
    }

    // Blocks the writer thread inside the engine step on any order whose client order id starts with HOLD.
    private static final class Gate implements CommandInterceptor {
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void beforeEngine(exchangeserver.command.Command command) {
            if (command instanceof NewOrderCommand n && n.request().getClientOrderId().startsWith("HOLD")) {
                reached.countDown();
                try {
                    if (!release.await(WAIT.toSeconds(), TimeUnit.SECONDS)) {
                        throw new IllegalStateException("gate never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }

        void awaitReached() throws InterruptedException {
            assertTrue(reached.await(WAIT.toSeconds(), TimeUnit.SECONDS), "writer never reached the gate");
        }
    }

    @Nested
    class HandIn {

        @Test
        void manyThreadsEachGetTheirOwnResult() throws Exception {
            ExchangeService service = start(CommandInterceptor.NONE);
            int threads = 8;
            int each = 500;
            List<List<CompletableFuture<Response>>> perThread = new ArrayList<>();
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                List<CompletableFuture<Response>> mine = Collections.synchronizedList(new ArrayList<>());
                perThread.add(mine);
                String participant = "p" + t;
                Side side = t % 2 == 0 ? Side.BUY : Side.SELL;      // half buy, half sell: lots of trades
                Thread worker = new Thread(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < each; i++) {
                            mine.add(service.submit(order(participant, participant + "-" + i, side, 100)));
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "submitter-" + t);
                worker.start();
                workers.add(worker);
            }
            go.countDown();
            for (Thread w : workers) {
                w.join(WAIT.toMillis());
                assertFalse(w.isAlive());
            }

            Set<Long> orderIds = new HashSet<>();
            for (int t = 0; t < threads; t++) {
                List<CompletableFuture<Response>> mine = perThread.get(t);
                assertEquals(each, mine.size());
                for (int i = 0; i < each; i++) {
                    Response r = await(mine.get(i));
                    assertEquals("p" + t + "-" + i, r.clientOrderId(), "a caller got someone else's result");
                    assertTrue(orderIds.add(r.orderId()), "order id issued twice");
                }
            }
            BookSnapshot before = await(service.snapshot(Integer.MAX_VALUE));

            assertTrue(service.stop(WAIT));
            assertEquals(threads * each + 1, service.processed(), "every order, plus the snapshot read");
            try (Exchange replayed = Exchange.open(journal(), clock)) {
                assertEquals(threads * each, replayed.lastSequenceNumber());
                assertEquals(before, replayed.snapshot(Integer.MAX_VALUE), "the journal alone rebuilds the book");
            }
        }

        @Test
        void readsGoThroughTheQueueInOrder() throws Exception {
            ExchangeService service = start(CommandInterceptor.NONE);
            service.submit(order("a", "1", Side.BUY, 100));
            BookSnapshot s = await(service.snapshot(5));
            assertEquals(1, s.sequenceNumber(), "no wait was needed: the snapshot queued behind the submit");
            assertEquals(1, s.bids().size());
        }

        @Test
        void startReplaysAnExistingJournal() throws Exception {
            try (Exchange ex = Exchange.open(journal(), clock)) {
                ex.submit(order("a", "1", Side.BUY, 100));
            }
            ExchangeService service = start(CommandInterceptor.NONE);
            assertEquals(1, await(service.snapshot(5)).bids().get(0).volume());
            assertEquals(2, await(service.submit(order("a", "2", Side.BUY, 100))).orderId());
        }

        @Test
        void nullOrderIsRefusedOnTheCallersThread() throws IOException {
            ExchangeService service = start(CommandInterceptor.NONE);
            assertThrows(NullPointerException.class, () -> service.submit(null));
            assertThrows(IllegalArgumentException.class, () -> service.snapshot(0));
        }

        // Review finding 1: the caller's future used to be the writer's own; cancelling it while queued made the
        // writer skip a command it had already journalled, so the live book and the replayed book differed.
        @Test
        void cancellingTheCallersFutureDoesNotStopAJournalledCommand() throws Exception {
            Gate gate = new Gate();
            ExchangeService service = start(gate);
            service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            CompletableFuture<Response> mine = service.submit(order("a", "x", Side.BUY, 99));
            assertTrue(mine.cancel(true));
            CompletableFuture<Response> timedOut = service.submit(order("a", "y", Side.BUY, 98)).orTimeout(1, TimeUnit.MILLISECONDS);
            assertThrows(ExecutionException.class, () -> timedOut.get(WAIT.toSeconds(), TimeUnit.SECONDS));
            gate.release.countDown();

            BookSnapshot live = await(service.snapshot(10));
            assertEquals(List.of(100L, 99L, 98L), live.bids().stream().map(l -> l.price()).toList(),
                    "both orders were journalled, so both must be in the live book");
            assertTrue(service.stop(WAIT));
            try (Exchange replayed = Exchange.open(journal(), clock)) {
                assertEquals(live, replayed.snapshot(10));
            }
        }

        // Review finding 8: continuations a caller attaches must not run on (and block) the writer thread.
        @Test
        void callerContinuationsRunOffTheWriterAndCannotStallIt() throws Exception {
            ExchangeService service = start(CommandInterceptor.NONE);
            CountDownLatch release = new CountDownLatch(1);
            List<String> threads = Collections.synchronizedList(new ArrayList<>());
            CompletableFuture<Void> slow = service.submit(order("a", "1", Side.BUY, 100)).thenAccept(r -> {
                threads.add(Thread.currentThread().getName());
                try {
                    release.await(WAIT.toSeconds(), TimeUnit.SECONDS);      // a "blocking database call"
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            // The writer is not held up by the blocked continuation.
            assertEquals(OrderStatus.RESTED, await(service.submit(order("a", "2", Side.BUY, 100))).status());
            release.countDown();
            await(slow);
            assertFalse(threads.get(0).equals("exchange-writer"), threads.toString());
        }

        @Test
        void callbackRunsOnTheWriterBeforeTheListener() throws Exception {
            List<String> events = Collections.synchronizedList(new ArrayList<>());
            ExchangeService service = start(ExchangeService.Settings.defaults(), CommandInterceptor.NONE,
                    out -> events.add("listener " + out.sequenceNumber() + " " + Thread.currentThread().getName()));
            CountDownLatch done = new CountDownLatch(2);
            for (int i = 1; i <= 2; i++) {
                service.submit(order("a", "c" + i, Side.BUY, 100), (r, e) -> {
                    events.add("reply " + r.orderId() + " " + Thread.currentThread().getName());
                    done.countDown();
                });
            }
            assertTrue(done.await(WAIT.toSeconds(), TimeUnit.SECONDS));
            await(service.snapshot(1));         // happens-before: the listener calls are done too
            assertEquals(List.of(
                    "reply 1 exchange-writer", "listener 1 exchange-writer",
                    "reply 2 exchange-writer", "listener 2 exchange-writer"), events);
        }
    }

    @Nested
    class Stop {

        @Test
        void drainsEverythingAcceptedAndRefusesTheRest() throws Exception {
            Gate gate = new Gate();
            ExchangeService service = start(gate);
            CompletableFuture<Response> held = service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            List<CompletableFuture<Response>> queued = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                queued.add(service.submit(order("a", "q" + i, Side.BUY, 100)));
            }

            Thread stopper = new Thread(() -> {
                try {
                    service.stop(WAIT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            stopper.start();
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (service.state() == ExchangeService.State.RUNNING && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertEquals(ExchangeService.State.STOPPING, service.state());
            assertEquals(ExchangeException.Reason.STOPPED, refusal(service.submit(order("a", "late", Side.BUY, 100))));

            gate.release.countDown();
            stopper.join(WAIT.toMillis());
            assertFalse(stopper.isAlive());
            assertEquals(OrderStatus.RESTED, await(held).status());
            for (CompletableFuture<Response> f : queued) {
                assertEquals(OrderStatus.RESTED, await(f).status(), "accepted before stop: must be processed");
            }
            assertEquals(ExchangeService.State.STOPPED, service.state());
        }

        @Test
        void isIdempotentAndReleasesTheJournal() throws Exception {
            int open = CommandJournal.openJournals();
            ExchangeService service = start(CommandInterceptor.NONE);
            assertEquals(open + 1, CommandJournal.openJournals());
            await(service.submit(order("a", "1", Side.BUY, 100)));
            assertTrue(service.stop(WAIT));
            assertTrue(service.stop(WAIT));
            assertEquals(ExchangeException.Reason.STOPPED, refusal(service.submit(order("a", "2", Side.BUY, 100))));
            assertEquals(open, CommandJournal.openJournals(), "the writer closed the journal on its way out");
        }

        @Test
        void reportsFalseWhenTheWriterDoesNotEndInTime() throws Exception {
            Gate gate = new Gate();
            ExchangeService service = start(gate);
            CompletableFuture<Response> held = service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            assertFalse(service.stop(Duration.ofMillis(100)), "the writer is still blocked");
            gate.release.countDown();
            assertTrue(service.stop(WAIT));
            assertEquals(OrderStatus.RESTED, await(held).status());
        }

        // Threads guide, Exercise 8: producers race a stop. No request may be lost: each one gets a result or
        // a refusal, and the writer ends.
        @RepeatedTest(5)
        void racingStopNeverLosesARequest() throws Exception {
            ExchangeService service = start(CommandInterceptor.NONE);
            int producers = 4;
            int each = 250;
            List<CompletableFuture<Response>> all = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger handedIn = new AtomicInteger();
            List<Thread> threads = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                String participant = "p" + p;
                Thread t = new Thread(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < each; i++) {
                            all.add(service.submit(order(participant, "c" + i, Side.BUY, 100)));
                            handedIn.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                t.start();
                threads.add(t);
            }
            go.countDown();
            while (handedIn.get() < producers * each / 3) {
                Thread.onSpinWait();
            }
            assertTrue(service.stop(WAIT), "writer did not end");
            for (Thread t : threads) {
                t.join(WAIT.toMillis());
                assertFalse(t.isAlive());
            }

            int processed = 0;
            int refused = 0;
            assertEquals(producers * each, all.size());
            for (CompletableFuture<Response> f : all) {
                try {
                    await(f);
                    processed++;
                } catch (ExecutionException e) {
                    assertEquals(ExchangeException.Reason.STOPPED, ((ExchangeException) e.getCause()).getReason());
                    refused++;
                }
            }
            assertEquals(producers * each, processed + refused);
            assertEquals(processed, service.processed());
            assertTrue(processed > 0);
        }
    }

    @Nested
    class Backpressure {

        @Test
        void fullQueueRefusesWithBusyAndJournalsNothing() throws Exception {
            Gate gate = new Gate();
            ExchangeService service = start(ExchangeService.Settings.defaults().withQueueCapacity(3), gate, OutputListener.NONE);
            CompletableFuture<Response> held = service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();                // the writer has taken it: the queue is empty again
            List<CompletableFuture<Response>> accepted = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                accepted.add(service.submit(order("a", "q" + i, Side.BUY, 100)));
            }
            // submit() returned, so the caller did not block; the refusal arrives without the writer's help.
            CompletableFuture<Response> busy = service.submit(order("a", "busy", Side.BUY, 100));
            assertEquals(ExchangeException.Reason.BUSY, refusal(busy));

            gate.release.countDown();
            await(held);
            for (CompletableFuture<Response> f : accepted) {
                await(f);
            }
            assertTrue(service.stop(WAIT));
            assertEquals(4, service.processed());
        }
    }

    @Nested
    class Failures {

        @Test
        void poisonIsQuarantinedAndTheServiceCarriesOn() throws Exception {
            CommandInterceptor poison = c -> {
                if (c instanceof NewOrderCommand n && n.request().getClientOrderId().equals("POISON")) {
                    throw new IllegalStateException("simulated bug");
                }
            };
            ExchangeService service = start(poison);
            assertEquals(ExchangeException.Reason.INTERNAL_ERROR, refusal(service.submit(order("a", "POISON", Side.BUY, 100))));
            assertEquals(OrderStatus.RESTED, await(service.submit(order("a", "fine", Side.BUY, 100))).status());
            assertEquals(ExchangeService.State.RUNNING, service.state());
        }

        @Test
        void fatalErrorHaltsAndEveryoneWaitingIsTold() throws Exception {
            Gate gate = new Gate();
            CommandInterceptor fatal = c -> {
                gate.beforeEngine(c);
                if (c instanceof NewOrderCommand n && n.request().getClientOrderId().equals("FATAL")) {
                    throw new OutOfMemoryError("simulated");
                }
            };
            ExchangeService service = start(fatal);
            CompletableFuture<Response> held = service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            CompletableFuture<Response> boom = service.submit(order("a", "FATAL", Side.BUY, 100));
            List<CompletableFuture<Response>> behind = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                behind.add(service.submit(order("a", "b" + i, Side.BUY, 100)));
            }
            gate.release.countDown();

            assertEquals(OrderStatus.RESTED, await(held).status());
            assertEquals(ExchangeException.Reason.HALTED, refusal(boom));
            for (CompletableFuture<Response> f : behind) {
                assertEquals(ExchangeException.Reason.HALTED, refusal(f));
            }
            assertTrue(service.stop(WAIT));
            assertEquals(ExchangeService.State.HALTED, service.state());
            assertInstanceOf(OutOfMemoryError.class, service.haltCause());
            assertEquals(ExchangeException.Reason.HALTED, refusal(service.submit(order("a", "after", Side.BUY, 100))));
        }

        @Test
        void haltRefusesWhatIsStillQueuedAndClosesTheJournal() throws Exception {
            int open = CommandJournal.openJournals();
            Gate gate = new Gate();
            CommandInterceptor fatal = c -> {
                gate.beforeEngine(c);
                if (c instanceof NewOrderCommand n && n.request().getClientOrderId().equals("FATAL")) {
                    throw new OutOfMemoryError("simulated");
                }
            };
            // Batches of one: the requests behind FATAL are still in the queue, not in its batch.
            ExchangeService service = start(ExchangeService.Settings.defaults().withMaxBatchSize(1), fatal, OutputListener.NONE);
            service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            CompletableFuture<Response> boom = service.submit(order("a", "FATAL", Side.BUY, 100));
            List<CompletableFuture<Response>> queued = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                queued.add(service.submit(order("a", "q" + i, Side.BUY, 100)));
            }
            gate.release.countDown();
            assertEquals(ExchangeException.Reason.HALTED, refusal(boom));
            for (CompletableFuture<Response> f : queued) {
                assertEquals(ExchangeException.Reason.HALTED, refusal(f));
            }
            assertTrue(service.stop(WAIT));
            assertEquals(open, CommandJournal.openJournals());
            assertEquals(2, Files.readAllLines(journal()).size() - 1, "HOLD and FATAL were journalled; the queued ones never were");
        }
    }

    @Nested
    class Batching {

        @Test
        void waitingRequestsShareOneBatch() throws Exception {
            Gate gate = new Gate();
            ExchangeService service = start(gate);
            CompletableFuture<Response> held = service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            List<CompletableFuture<Response>> waiting = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                waiting.add(service.submit(order("a", "w" + i, Side.BUY, 100)));
            }
            gate.release.countDown();
            await(held);
            for (CompletableFuture<Response> f : waiting) {
                await(f);
            }
            assertTrue(service.stop(WAIT));
            assertEquals(50, service.largestBatch());
            assertEquals(2, service.batches());
        }

        @Test
        void maxBatchSizeOneIsFsyncPerCommand() throws Exception {
            Gate gate = new Gate();
            ExchangeService service = start(ExchangeService.Settings.defaults().withMaxBatchSize(1), gate, OutputListener.NONE);
            service.submit(order("a", "HOLD", Side.BUY, 100));
            gate.awaitReached();
            List<CompletableFuture<Response>> waiting = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                waiting.add(service.submit(order("a", "w" + i, Side.BUY, 100)));
            }
            gate.release.countDown();
            for (CompletableFuture<Response> f : waiting) {
                await(f);
            }
            assertTrue(service.stop(WAIT));
            assertEquals(1, service.largestBatch());
            assertEquals(11, service.batches());
        }

        @Test
        void lingerCollectsLateArrivalsIntoTheSameBatch() throws Exception {
            ExchangeService service = start(ExchangeService.Settings.defaults().withLinger(Duration.ofMillis(300)),
                    CommandInterceptor.NONE, OutputListener.NONE);
            CompletableFuture<Response> first = service.submit(order("a", "1", Side.BUY, 100));
            Thread.sleep(50);   // well inside the linger window; only affects batching, never correctness
            CompletableFuture<Response> second = service.submit(order("a", "2", Side.BUY, 100));
            await(first);
            await(second);
            assertTrue(service.stop(WAIT));
            assertEquals(1, service.batches());
            assertEquals(2, service.largestBatch());
        }

        @Test
        void stopDuringLingerStillDrains() throws Exception {
            ExchangeService service = start(ExchangeService.Settings.defaults().withLinger(Duration.ofSeconds(5)),
                    CommandInterceptor.NONE, OutputListener.NONE);
            CompletableFuture<Response> f = service.submit(order("a", "1", Side.BUY, 100));
            long t0 = System.nanoTime();
            assertTrue(service.stop(WAIT));
            assertTrue(System.nanoTime() - t0 < Duration.ofSeconds(4).toNanos(), "STOP ends the linger early");
            assertEquals(OrderStatus.RESTED, await(f).status());
        }
    }

    @Test
    void settingsAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> ExchangeService.Settings.defaults().withQueueCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> ExchangeService.Settings.defaults().withMaxBatchSize(0));
        assertThrows(IllegalArgumentException.class, () -> ExchangeService.Settings.defaults().withLinger(Duration.ofMillis(-1)));
        assertSame(clock, ExchangeService.Settings.defaults().withClock(clock).clock());
    }
}
