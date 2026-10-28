package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.engine.CancelAllOutput;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.Execution;
import exchangeserver.engine.OrderOutput;
import exchangeserver.engine.OrderStatus;
import exchangeserver.engine.Response;
import exchangeserver.engine.Trade;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputLogTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    private static NewOrderCommand buy(long seq, String participant) {
        return new NewOrderCommand(seq, T, seq, OrderRequest.limit(participant, "c" + seq, Side.BUY, 100, 2));
    }

    private static OrderOutput filledAgainst(NewOrderCommand c, String maker) {
        Response r = new Response(c.orderId(), c.request().getClientOrderId(), OrderStatus.FILLED, null, 2, 0,
                List.of(new Trade(100, 2, c.orderId(), 99, Side.BUY, T)));
        return new OrderOutput(c.sequenceNumber(), T, r, List.of(new Execution(maker, 99, "m", Side.SELL, 100, 2, 0)), List.of());
    }

    @Test
    void eachParticipantIsNumberedFromOneRequesterFirst() {
        OutputLog log = new OutputLog(10);
        NewOrderCommand c = buy(1, "taker");
        List<PrivateEvent> events = log.record(c, filledAgainst(c, "maker"));
        assertEquals(2, events.size());
        assertTrue(events.get(0) instanceof PrivateEvent.OrderReply r && r.participant().equals("taker") && r.oseq() == 1);
        assertTrue(events.get(1) instanceof PrivateEvent.MakerFill f && f.participant().equals("maker") && f.oseq() == 1);
        log.record(new CancelOrderCommand(2, T, "maker", 5), new CancelOutput(2, T, CancelResult.unknown(5), List.of()));
        assertEquals(2, log.lastOseq("maker"));
        assertEquals(1, log.lastOseq("taker"));
        assertEquals(0, log.lastOseq("nobody"));
    }

    @Test
    void aCancelAllNumbersEachCancel() {
        OutputLog log = new OutputLog(10);
        List<PrivateEvent> events = log.record(new CancelAllCommand(1, T, "a"), new CancelAllOutput(1, T, "a",
                List.of(CancelResult.cancelled(3, 1), CancelResult.cancelled(4, 2)), List.of()));
        assertEquals(List.of(1L, 2L), events.stream().map(PrivateEvent::oseq).toList());
        assertTrue(events.stream().allMatch(e -> e instanceof PrivateEvent.CancelReply r && r.byCancelAll()));
    }

    @Test
    void onlyTheMostRecentEventsAreRetained() {
        OutputLog log = new OutputLog(2);
        for (long s = 1; s <= 5; s++) {
            NewOrderCommand c = buy(s, "a");
            log.record(c, new OrderOutput(s, T, new Response(s, "c" + s, OrderStatus.RESTED, null, 0, 2, List.of()), List.of(), List.of()));
        }
        ResendResult all = log.since("a", 1);
        assertEquals(4, all.firstAvailable());
        assertEquals(5, all.lastOseq());
        assertEquals(List.of(4L, 5L), all.events().stream().map(PrivateEvent::oseq).toList());
        assertEquals(List.of(5L), log.since("a", 5).events().stream().map(PrivateEvent::oseq).toList());
        assertTrue(log.since("a", 6).events().isEmpty());
        assertEquals(new ResendResult(List.of(), 1, 0), log.since("nobody", 1));
    }

    @Test
    void resumingContinuesTheNumberingWithNothingRetained() {
        OutputLog log = OutputLog.resume(10, Map.of("a", 7L));
        assertEquals(7, log.lastOseq("a"));
        assertEquals(8, log.since("a", 1).firstAvailable());
        NewOrderCommand c = buy(1, "a");
        assertEquals(8, log.record(c, new OrderOutput(1, T, new Response(1, "c1", OrderStatus.RESTED, null, 0, 2, List.of()),
                List.of(), List.of())).get(0).oseq());
        assertEquals(Map.of("a", 8L), log.lastOseqs());
    }

    @Test
    void aMismatchedOutputIsABug() {
        OutputLog log = new OutputLog(10);
        assertThrows(IllegalStateException.class,
                () -> log.record(buy(1, "a"), new CancelOutput(1, T, CancelResult.unknown(1), List.of())));
        assertThrows(IllegalArgumentException.class, () -> new OutputLog(0));
    }
}
