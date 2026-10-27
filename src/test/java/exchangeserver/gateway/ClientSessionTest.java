package exchangeserver.gateway;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

// The session's threads and routing are covered end to end in TcpGatewayTest; this covers line reading.
class ClientSessionTest {

    private static InputStream in(String s) {
        return new ByteArrayInputStream(s.getBytes(US_ASCII));
    }

    @Nested
    class ReadLine {

        private final String max = "x".repeat(Protocol.MAX_LINE_LENGTH);

        @Test
        void lfAndCrlfBothEndALine() throws IOException {
            InputStream s = in("PING\nQUIT\r\n");
            assertEquals("PING", ClientSession.readLine(s));
            assertEquals("QUIT", ClientSession.readLine(s));
            assertNull(ClientSession.readLine(s));
        }

        // Review finding 10: the '\r' of "\r\n" used to count toward the limit.
        @Test
        void aLineOfExactlyTheLimitIsAcceptedWithEitherEnding() throws IOException {
            assertEquals(max, ClientSession.readLine(in(max + "\n")));
            assertEquals(max, ClientSession.readLine(in(max + "\r\n")));
        }

        @Test
        void oneByteOverTheLimitIsRefusedWithEitherEnding() {
            assertThrows(ProtocolException.class, () -> ClientSession.readLine(in(max + "y\n")));
            assertThrows(ProtocolException.class, () -> ClientSession.readLine(in(max + "y\r\n")));
            assertThrows(ProtocolException.class, () -> ClientSession.readLine(in(max + "\r\r\n")));
        }

        @Test
        void anEndlessLineIsRefusedWithoutBufferingIt() {
            assertThrows(ProtocolException.class, () -> ClientSession.readLine(in("y".repeat(1_000_000))));
        }

        @Test
        void anUnterminatedLastLineIsDropped() throws IOException {
            assertNull(ClientSession.readLine(in("PING")));
        }
    }
}
