package exchangeserver.journal;

import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.CheckCommand;
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

    // The record as written: text, ",", CRC-32C of the text, "\n" (D69).
    private static String sealed(String text) {
        return text + "," + Checksums.crcHex(text) + "\n";
    }

    // A sealed record without its "\n", as decode() receives it.
    private static String record(String text) {
        String s = sealed(text);
        return s.substring(0, s.length() - 1);
    }

    @Nested
    class RoundTrip {

        @Test
        void newLimitOrder() {
            Command c = new NewOrderCommand(1, T, 7, OrderRequest.limit("alice", "a-1.x", Side.BUY, 100, 10));
            assertEquals(sealed("NEW,1,2026-01-01T10:00:00.123456789Z,7,alice,a-1.x,BUY,LIMIT,10,100"), JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void marketOrderPriceIsADash() {
            Command c = new NewOrderCommand(2, T, 8, OrderRequest.market("bob", "m1", Side.SELL, 5));
            assertEquals(sealed("NEW,2,2026-01-01T10:00:00.123456789Z,8,bob,m1,SELL,MARKET,5,-"), JournalCodec.encode(c));
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
            assertEquals(sealed("CANCEL,3,2026-01-01T10:00:00.123456789Z,alice,7"), JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void cancelAll() {
            Command c = new CancelAllCommand(5, T, "alice");
            assertEquals(sealed("CANCEL_ALL,5,2026-01-01T10:00:00.123456789Z,alice"), JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void skip() {
            Command c = new SkipCommand(9, T, 4);
            assertEquals(sealed("SKIP,9,2026-01-01T10:00:00.123456789Z,4"), JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }

        @Test
        void check() {
            Command c = new CheckCommand(10, T, "00ff00ff00ff00ff");
            assertEquals(sealed("CHECK,10,2026-01-01T10:00:00.123456789Z,00ff00ff00ff00ff"), JournalCodec.encode(c));
            assertEquals(c, roundTrip(c));
        }
    }

    @Nested
    class Header {

        @Test
        void roundTripWithTheBaseSequence() {
            String h = JournalCodec.encodeHeader(5, 42);
            assertEquals(sealed("HEADER," + JournalCodec.FORMAT_VERSION + ",5,42"), h);
            assertEquals(new JournalCodec.Header(5, 42), JournalCodec.decodeHeader(h.strip()));
        }

        @Test
        void otherFormatVersionIsRefused() {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> JournalCodec.decodeHeader(record("HEADER,2,1")));
            assertTrue(e.getMessage().contains("format version 2"), e.getMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "NEW,1", "HEADER,3", "HEADER,x,1,0", "HEADER,3,1,0,extra", "HEADER,3,1,-1", "HEADER,3,1"})
        void malformedHeaderIsRefused(String text) {
            assertThrows(IllegalStateException.class, () -> JournalCodec.decodeHeader(record(text)));
        }

        @Test
        void unsealedHeaderIsRefused() {
            assertThrows(IllegalStateException.class, () -> JournalCodec.decodeHeader("HEADER,3,1,0"));
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
                "CANCEL_ALL,1,2026-01-01T10:00:00Z",
                "CANCEL_ALL,1,2026-01-01T10:00:00Z,alice,extra",
                "CHECK,3,2026-01-01T10:00:00Z,0123456789abcdef,extra",
                "CHECK,3,2026-01-01T10:00:00Z",
                "CANCEL_ALL,1,2026-01-01T10:00:00Z,a b",                          // invalid participant
                "SKIP,4,2026-01-01T10:00:00Z,3,extra",
                "SKIP,4,2026-01-01T10:00:00Z",
                "CANCEL,1,2026-01-01T10:00:00Z,,7",                               // empty participant
                "SKIP,3,2026-01-01T10:00:00Z,3",                                  // skips itself
                "CHECK,3,2026-01-01T10:00:00Z,XYZ",                               // not a digest
        })
        void isAnIllegalStateWithTheRecordNumber(String text) {
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> JournalCodec.decode(record(text), 42));
            assertTrue(e.getMessage().contains("record 42"), e.getMessage());
        }

        // D69: a bit flip that still parses changes history; the checksum catches it.
        @Test
        void aChangedCharacterFailsTheChecksum() {
            String good = record("NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10,100");
            String flipped = good.replace(",100,", ",900,");
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> JournalCodec.decode(flipped, 3));
            assertTrue(e.getCause().getMessage().contains("checksum mismatch"), e.getCause().getMessage());
        }

        @Test
        void aRecordWithoutChecksumIsCorrupt() {
            assertThrows(IllegalStateException.class,
                    () -> JournalCodec.decode("NEW,1,2026-01-01T10:00:00Z,7,alice,c,BUY,LIMIT,10,100", 3));
        }

        // D69 (M7): a CRLF conversion gets a message that says what happened.
        @Test
        void aCarriageReturnIsReportedAsALineEndingConversion() {
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> JournalCodec.decode(record("CANCEL,1,2026-01-01T10:00:00Z,alice,7") + "\r", 3));
            assertTrue(e.getCause().getMessage().contains("CRLF"), e.getCause().getMessage());
            IllegalStateException h = assertThrows(IllegalStateException.class,
                    () -> JournalCodec.decodeHeader(record("HEADER,3,1,0") + "\r"));
            assertTrue(h.getMessage().contains("CRLF"), h.getMessage());
        }
    }

    @Test
    void encodeRejectsNull() {
        assertThrows(NullPointerException.class, () -> JournalCodec.encode(null));
    }
}
