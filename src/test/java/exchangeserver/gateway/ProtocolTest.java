package exchangeserver.gateway;

import exchangeserver.book.CancelResult;
import exchangeserver.book.LevelUpdate;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.Execution;
import exchangeserver.engine.OrderStatus;
import exchangeserver.engine.RejectReason;
import exchangeserver.engine.Response;
import exchangeserver.engine.Trade;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    @Nested
    class Parse {

        @Test
        void login() {
            assertEquals(new ClientMessage.Login("alice"), Protocol.parse("LOGIN alice"));
        }

        @Test
        void pricedOrder() {
            assertEquals(new ClientMessage.NewOrder("c1", Side.BUY, OrderType.LIMIT, 10, OptionalLong.of(100)),
                    Protocol.parse("NEW c1 BUY LIMIT 10 100"));
            assertEquals(new ClientMessage.NewOrder("c2", Side.SELL, OrderType.POST_ONLY, 1, OptionalLong.of(5)),
                    Protocol.parse("NEW c2 SELL POST_ONLY 1 5"));
        }

        @Test
        void marketOrderHasNoPriceField() {
            assertEquals(new ClientMessage.NewOrder("m", Side.SELL, OrderType.MARKET, 3, OptionalLong.empty()),
                    Protocol.parse("NEW m SELL MARKET 3"));
            assertThrows(ProtocolException.class, () -> Protocol.parse("NEW m SELL MARKET 3 100"));
        }

        @Test
        void otherMessages() {
            assertEquals(new ClientMessage.Cancel(7), Protocol.parse("CANCEL 7"));
            assertEquals(new ClientMessage.Subscribe(), Protocol.parse("SUBSCRIBE"));
            assertEquals(new ClientMessage.Unsubscribe(), Protocol.parse("UNSUBSCRIBE"));
            assertEquals(new ClientMessage.Ping(), Protocol.parse("PING"));
            assertEquals(new ClientMessage.Quit(), Protocol.parse("QUIT"));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "", "HELLO", "login alice", "LOGIN", "LOGIN a b", "NEW", "NEW c1 BUY LIMIT 10",
                "NEW c1 BUY LIMIT 10 100 extra", "NEW c1 HOLD LIMIT 10 100", "NEW c1 BUY STOP 10 100",
                "NEW c1 BUY LIMIT ten 100", "NEW c1 BUY LIMIT 10 1.5", "CANCEL", "CANCEL x", "CANCEL 1 2",
                "PING now", "NEW  c1 BUY LIMIT 10 100",
        })
        void malformedLinesAreProtocolErrors(String line) {
            assertThrows(ProtocolException.class, () -> Protocol.parse(line));
        }

        @Test
        void syntaxOnlySemanticsAreCheckedLater() {
            // Parses fine; OrderRequest (in the session) rejects the zero quantity and the negative price.
            Protocol.parse("NEW c1 BUY LIMIT 0 -5");
            Protocol.parse("CANCEL -3");
        }
    }

    @Nested
    class Format {

        @Test
        void takerFillsCountLeavesDown() {
            Response r = new Response(9, "c9", OrderStatus.RESTED, null, 5, 5,
                    List.of(new Trade(100, 2, 9, 1, Side.BUY, T), new Trade(101, 3, 9, 2, Side.BUY, T)));
            List<String> lines = new ArrayList<>();
            Protocol.takerFills(r, Side.BUY, 10, lines);
            assertEquals(List.of(
                    "FILL orderId=9 clOrdId=c9 side=BUY price=100 qty=2 leaves=8 liquidity=TAKER",
                    "FILL orderId=9 clOrdId=c9 side=BUY price=101 qty=3 leaves=5 liquidity=TAKER"), lines);
        }

        @Test
        void makerFill() {
            assertEquals("FILL orderId=1 clOrdId=a side=SELL price=100 qty=2 leaves=3 liquidity=MAKER",
                    Protocol.makerFill(new Execution("alice", 1, "a", Side.SELL, 100, 2, 3)));
        }

        @Test
        void orderLine() {
            assertEquals("ORDER orderId=9 clOrdId=c status=RESTED filled=0 remaining=4",
                    Protocol.order(new Response(9, "c", OrderStatus.RESTED, null, 0, 4, List.of())));
            assertEquals("ORDER orderId=3 clOrdId=c status=REJECTED filled=0 remaining=4 reason=DUPLICATE_CLIENT_ORDER_ID",
                    Protocol.order(new Response(3, "c", OrderStatus.REJECTED, RejectReason.DUPLICATE_CLIENT_ORDER_ID, 0, 4, List.of())));
        }

        @Test
        void cancelLines() {
            assertEquals("CANCELLED orderId=4 qty=6", Protocol.cancel(CancelResult.cancelled(4, 6)));
            assertEquals("CANCEL_REJECT orderId=4 reason=UNKNOWN_ORDER", Protocol.cancel(CancelResult.unknown(4)));
        }

        @Test
        void bookLine() {
            BookSnapshot s = new BookSnapshot(12, 100,
                    List.of(new LevelUpdate(Side.BUY, 99, 5, 1), new LevelUpdate(Side.BUY, 98, 7, 2)), List.of());
            assertEquals("BOOK seq=12 last=100 bids=99x5,98x7 asks=-", Protocol.book(s));
        }

        @Test
        void marketDataLines() {
            assertEquals("TRADE seq=5 price=100 qty=2 aggressor=SELL", Protocol.trade(5, new Trade(100, 2, 1, 2, Side.SELL, T)));
            assertEquals("LEVEL seq=5 side=BUY price=100 qty=0 orders=0", Protocol.level(5, new LevelUpdate(Side.BUY, 100, 0, 0)));
        }

        @Test
        void errorIsAlwaysOneLine() {
            assertEquals("ERROR a b c", Protocol.error("a\nb\rc"));
        }
    }
}
