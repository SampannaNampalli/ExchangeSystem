package exchangeserver.bench;

import exchangeserver.core.ExchangeService;
import exchangeserver.core.OutputListener;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.engine.Response;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/*

Roadmap step 4C / 6: fsync per command vs group commit with a timeout vs smart batching (D37, D52).

Each JMH thread is one client in a closed loop: submit, wait for the response, repeat. That is how latency and
throughput trade off for real clients. Run with different thread counts (-t) to see batching kick in:

    mvnw -Pbench verify -Djmh.args="BatchingBenchmark -f 1 -wi 2 -w 2s -i 3 -r 3s -t 1"
    ... -t 16, -t 64

Modes: PER_COMMAND (maxBatchSize 1), LINGER_1MS (group commit: wait up to 1 ms for more), SMART (take what is
waiting, no timer). The journal is written under target/, i.e. on the project's real disk: fsync cost is the
whole point.

 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BatchingBenchmark {

    public enum BatchMode {
        PER_COMMAND, LINGER_1MS, SMART
    }

    @Param({"PER_COMMAND", "LINGER_1MS", "SMART"})
    BatchMode mode;

    private ExchangeService service;
    private Path journal;

    @Setup(Level.Trial)
    public void start() throws Exception {
        Path dir = Files.createDirectories(Path.of("target", "bench"));
        journal = Files.createTempFile(dir, "batching-" + mode + "-", ".journal");
        Files.delete(journal);
        ExchangeService.Settings settings = ExchangeService.Settings.defaults();
        settings = switch (mode) {
            case PER_COMMAND -> settings.withMaxBatchSize(1);
            case LINGER_1MS -> settings.withLinger(Duration.ofMillis(1));
            case SMART -> settings;
        };
        service = ExchangeService.start(journal, OutputListener.NONE, settings);
    }

    @TearDown(Level.Trial)
    public void stop() throws Exception {
        service.stop(Duration.ofSeconds(30));
        System.out.printf("%n[%s] batches=%d commands=%d largest=%d avg=%.1f%n", mode, service.batches(),
                service.processed(), service.largestBatch(), service.processed() / (double) Math.max(1, service.batches()));
        Files.deleteIfExists(journal);
    }

    @State(Scope.Thread)
    public static class Client {
        private static final AtomicInteger NEXT = new AtomicInteger();
        final String participant = "t" + NEXT.incrementAndGet();
        long n;
    }

    // Alternating buy and sell at one price: orders trade with each other, so the book (and every participant's
    // open-order count) stays tiny however long the benchmark runs.
    @Benchmark
    public Response submitAndWait(Client client) throws Exception {
        long n = client.n++;
        Side side = n % 2 == 0 ? Side.SELL : Side.BUY;
        OrderRequest r = OrderRequest.limit(client.participant, "c" + n, side, 100, 1);
        return service.submit(r).get(30, TimeUnit.SECONDS);
    }
}
