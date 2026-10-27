package exchangeserver.journal;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JournalCodecTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00.123456789Z");

    private static Command roundTrip(Command c) {
        String record = JournalCodec.encode(c);
        assertTrue(record.endsWith("\n"));
        assertEquals(1, record.chars().filter(ch -> ch == '\n').count());
        return JournalCodec.decode(record.substring(0, record.length() - 1), 1);
    }

    @Nested
    class RoundTrip {

        @Test
        void newLimitOrder() {
            Command c = new NewOrderCommand(1, T, 7, OrderRequest.limit("alice", "a-1.x", Side.BUY, 100, 10));
            assertEquals("NEW,1,2026-01-01T10:00:00.123456789Z,7,alice,a-1.x,BUY,LIMIT,10,100\n", JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void marketOrderPriceIsADash() {
            Command c = new NewOrderCommand(2, T, 8, OrderRequest.market("bob", "m1", Side.SELL, 5));
            assertEquals("NEW,2,2026-01-01T10:00:00.123456789Z,8,bob,m1,SELL,MARKET,5,-\n", JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void everyPricedType() {
            for (OrderRequest r : new OrderRequest[]{OrderRequest.ioc("a", "c", Side.BUY, 1, 2),
                    OrderRequest.fok("a", "c", Side.SELL, 3, 4), OrderRequest.postOnly("a", "c", Side.BUY, 5, 6)}) {
                Command c = new NewOrderCommand(3, T, 9, r);
                assertEquals(c, roundTrip(c));
            }
        }

        @Test
        void cancel() {
            Command c = new CancelOrderCommand(3, T, "alice", 7);
            assertEquals("CANCEL,3,2026-01-01T10:00:00.123456789Z,alice,7\n", JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void skip() {
            Command c = new SkipCommand(9, T, 4);
            assertEquals("SKIP,9,2026-01-01T10:00:00.123456789Z,4\n", JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }
    }

    @Nested
    class Header {

        @Test
        void roundTrip() {
            String h = JournalCodec.encodeHeader(5);
            assertEquals("HEADER," + JournalCodec.FORMAT_VERSION + ",5\n", h);
            assertEquals(5, JournalCodec.decodeHeader(h.strip()));
        }

        @Test
        void otherFormatVersionIsRefused() {
            assertThrows(IllegalStateException.class, () -> JournalCodec.decodeHeader("HEADER,1,1"));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "NEW,1", "HEADER,2", "HEADER,x,1", "HEADER,2,1,extra"})
        void malformedHeaderIsRefused(String record) {
            assertThrows(IllegalStateException.class, () -> JournalCodec.decodeHeader(record));
        }
    }

    @Nested
    class Corrupt {

        @ParameterizedTest
        @ValueSource(strings = {
                "",
                "BOGUS,1,2026-01-01T10:00:00Z",
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10",              // too few fields
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10,100,x",        // too many
                "NEW,x,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10,100",          // bad number
                "NEW,1,yesterday,7,alice,c,BUY,LIMIT,10,100",                     // bad timestamp
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,UP,LIMIT,10,100",           // bad side
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,STOP,10,100",           // bad type
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,0,100",           // fails OrderRequest validation
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,MARKET,10,100",         // market with a price
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10,-",            // limit without one
                "NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10,-1",           // the in-memory sentinel
                "NEW,1,2026-01-01T10:00:00Z,0,alice,c,BUY,LIMIT,10,100",          // id 0
                "CANCEL,1,2026-01-01T10:00:00Z,alice",
                "CANCEL,1,2026-01-01T10:00:00Z,alice,7,extra",
                "SKIP,4,2026-01-01T10:00:00Z,3,extra",
                "SKIP,4,2026-01-01T10:00:00Z",
                "CANCEL,1,2026-01-01T10:00:00Z,,7",                               // empty participant
                "SKIP,3,2026-01-01T10:00:00Z,3",                                  // skips itself
        })
        void isAnIllegalStateWithTheRecordNumber(String record) {
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> JournalCodec.decode(record, 42));
            assertTrue(e.getMessage().contains("record 42"), e.getMessage());
        }
    }

    @Test
    void encodeRejectsNull() {
        assertThrows(NullPointerException.class, () -> JournalCodec.encode(null));
    }
}
