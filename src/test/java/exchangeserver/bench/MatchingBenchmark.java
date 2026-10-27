package exchangeserver.bench;

import exchangeserver.book.OrderBook;
import exchangeserver.book.PriceLevel;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.MatchingEngine;
import exchangeserver.engine.OrderOutput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/*

Roadmap step 6: what does the engine cost, and does the O(log L) best-level lookup matter (D11)?

levels = number of price levels on EACH side of the book.

bestLevelLookup  TreeMap.firstEntry() + Optional: the cost a cached best-level pointer would remove.
restAndTake      two commands per op: a sell rests at the best ask (tail), then an IOC buy takes the head of that
                 level. One trade, one rest, one fill-removal; the book is the same size afterwards.
addAndCancel     two commands per op: a buy rests at a level in the middle of the book, then is cancelled
                 (O(1) unlink, but O(log L) to find the level on add).

The engine is rebuilt every iteration so the client-order-id map (it only grows, by design) stays small.

    mvnw -Pbench verify -Djmh.args="MatchingBenchmark -f 1 -wi 3 -w 1s -i 5 -r 1s"

 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MatchingBenchmark {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");
    private static final long MID = 1_000_000;

    @Param({"10", "1000", "100000"})
    int levels;

    private OrderBook book;
    private MatchingEngine engine;
    private long seq;
    private long id;
    private long bestAsk;
    private long middleBid;

    @Setup(Level.Iteration)
    public void buildBook() {
        book = new OrderBook();
        engine = new MatchingEngine(book);
        seq = 0;
        id = 0;
        // Asks MID+1 .. MID+levels, bids MID-1 .. MID-levels, two orders per level. No trades yet: no band.
        for (int i = 1; i <= levels; i++) {
            for (int k = 0; k < 2; k++) {
                newOrder(OrderRequest.limit("mm", "a" + i + "-" + k, Side.SELL, MID + i, 10));
                newOrder(OrderRequest.limit("mm", "b" + i + "-" + k, Side.BUY, MID - i, 10));
            }
        }
        bestAsk = MID + 1;
        middleBid = MID - Math.max(1, levels / 2);
        // One trade so the band is active (and inside it) for the measured part, as in production.
        newOrder(OrderRequest.ioc("t", "warmup", Side.BUY, bestAsk, 1));
    }

    private OrderOutput newOrder(OrderRequest request) {
        return engine.onNewOrder(new NewOrderCommand(++seq, T, ++id, request));
    }

    @Benchmark
    public Optional<PriceLevel> bestLevelLookup() {
        return book.bestLevel(Side.SELL);
    }

    @Benchmark
    public OrderOutput restAndTake() {
        newOrder(OrderRequest.limit("s", "s" + id, Side.SELL, bestAsk, 1));
        return newOrder(OrderRequest.ioc("b", "b" + id, Side.BUY, bestAsk, 1));
    }

    @Benchmark
    public CancelOutput addAndCancel() {
        OrderOutput out = newOrder(OrderRequest.limit("c", "c" + id, Side.BUY, middleBid, 1));
        return engine.onCancel(new CancelOrderCommand(++seq, T, "c", out.response().orderId()));
    }
}
