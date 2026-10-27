package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.book.LevelUpdate;
import exchangeserver.domain.Side;
import exchangeserver.engine.CancelAllOutput;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.OrderOutput;
import exchangeserver.engine.OrderStatus;
import exchangeserver.engine.Response;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class OutputDigestTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    private static String digestOf(EngineOutput... outputs) {
        OutputDigest d = new OutputDigest();
        for (EngineOutput o : outputs) {
            d.update(o);
        }
        return d.hex();
    }

    private static OrderOutput rested(long seq, long qty) {
        return new OrderOutput(seq, T, new Response(seq, "c", OrderStatus.RESTED, null, 0, qty, List.of()), List.of(),
                List.of(new LevelUpdate(Side.BUY, 100, qty, 1)));
    }

    @Test
    void theSameOutputsGiveTheSameDigestAndAnyDifferenceChangesIt() {
        assertEquals(digestOf(rested(1, 5), rested(2, 3)), digestOf(rested(1, 5), rested(2, 3)));
        assertNotEquals(digestOf(rested(1, 5), rested(2, 3)), digestOf(rested(1, 5), rested(2, 4)));
        assertNotEquals(digestOf(rested(1, 5), rested(2, 3)), digestOf(rested(2, 3), rested(1, 5)), "order matters");
    }

    @Test
    void everyKindOfOutputContributes() {
        CancelOutput cancel = new CancelOutput(3, T, CancelResult.cancelled(1, 5), List.of());
        CancelAllOutput all = new CancelAllOutput(3, T, "a", List.of(CancelResult.cancelled(1, 5)), List.of());
        assertNotEquals(digestOf(rested(1, 5)), digestOf(rested(1, 5), cancel));
        assertNotEquals(digestOf(rested(1, 5), cancel), digestOf(rested(1, 5), all));
    }

    // Every field of every output kind is in the digest: replay must reproduce each of them.
    @Test
    void eachFieldChangesTheDigest() {
        Response base = new Response(1, "c", OrderStatus.RESTED, null, 0, 5, List.of());
        String reference = digestOf(new OrderOutput(1, T, base, List.of(), List.of()));
        List<EngineOutput> variants = List.of(
                new OrderOutput(1, T, new Response(2, "c", OrderStatus.RESTED, null, 0, 5, List.of()), List.of(), List.of()),
                new OrderOutput(1, T, new Response(1, "d", OrderStatus.RESTED, null, 0, 5, List.of()), List.of(), List.of()),
                new OrderOutput(1, T, new Response(1, "c", OrderStatus.CANCELLED, null, 0, 5, List.of()), List.of(), List.of()),
                new OrderOutput(1, T, new Response(1, "c", OrderStatus.CANCELLED, exchangeserver.engine.Reason.SELF_TRADE_PREVENTED, 0, 5, List.of()), List.of(), List.of()),
                new OrderOutput(1, T, new Response(1, "c", OrderStatus.RESTED, null, 0, 6, List.of()), List.of(), List.of()),
                new OrderOutput(1, T, base, List.of(new exchangeserver.engine.Execution("m", 9, "x", Side.SELL, 100, 1, 0)), List.of()),
                new OrderOutput(1, T, base, List.of(), List.of(new LevelUpdate(Side.SELL, 100, 1, 1))),
                new OrderOutput(2, T, base, List.of(), List.of()),
                new OrderOutput(1, T.plusNanos(1), base, List.of(), List.of()));
        for (EngineOutput v : variants) {
            assertNotEquals(reference, digestOf(v), v.toString());
        }
    }

    @Test
    void cancelResultsAreInTheDigest() {
        assertNotEquals(digestOf(new CancelOutput(3, T, CancelResult.cancelled(1, 5), List.of())),
                digestOf(new CancelOutput(3, T, CancelResult.cancelled(1, 4), List.of())));
        assertNotEquals(digestOf(new CancelAllOutput(3, T, "a", List.of(CancelResult.cancelled(1, 5)), List.of())),
                digestOf(new CancelAllOutput(3, T, "a", List.of(CancelResult.cancelled(2, 5)), List.of())));
        assertNotEquals(digestOf(new CancelAllOutput(3, T, "a", List.of(), List.of())),
                digestOf(new CancelAllOutput(3, T, "b", List.of(), List.of())));
    }

    @Test
    void itResumesFromItsHexForm() {
        OutputDigest d = new OutputDigest();
        d.update(rested(1, 5));
        OutputDigest resumed = OutputDigest.resume(d.hex());
        d.update(rested(2, 3));
        resumed.update(rested(2, 3));
        assertEquals(d.hex(), resumed.hex());
        assertEquals(16, d.hex().length());
    }
}
