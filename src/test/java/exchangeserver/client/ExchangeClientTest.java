package exchangeserver.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExchangeClientTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    // A server that writes `lines` to whoever connects, then keeps the connection open.
    private static ExchangeClient clientReceiving(ServerSocket server, String lines) throws IOException {
        ExchangeClient client = new ExchangeClient("localhost", server.getLocalPort());
        Socket accepted = server.accept();
        OutputStream out = accepted.getOutputStream();
        out.write(lines.getBytes(US_ASCII));
        out.flush();
        return client;
    }

    // L4: removeAll used to drop every equal line, including a later one nobody had consumed yet.
    @Test
    void linesUntilConsumesExactlyTheLinesItReturns() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             ExchangeClient c = clientReceiving(server, "X\nPONG\nY\nPONG\n")) {
            assertEquals("Y", c.awaitPrefix("Y", WAIT));                    // X and the first PONG are skipped
            assertEquals(List.of("X", "PONG"), c.linesUntil(l -> l.equals("PONG"), WAIT));
            assertEquals(List.of("PONG"), c.linesUntil(l -> l.equals("PONG"), WAIT), "the second PONG is still there");
        }
    }

    @Test
    void aTimeoutKeepsTheLinesForTheNextCall() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             ExchangeClient c = clientReceiving(server, "A\nB\n")) {
            assertEquals("B", c.awaitPrefix("B", WAIT));
            assertThrows(IllegalStateException.class, () -> c.linesUntil(l -> l.equals("C"), Duration.ofMillis(200)));
            assertEquals("A", c.awaitPrefix("A", WAIT));
        }
    }

    @Test
    void fieldReadsKeyValuePairs() {
        assertEquals("7", ExchangeClient.field("ORDER oseq=1 orderId=7 status=RESTED", "orderId"));
        assertThrows(IllegalArgumentException.class, () -> ExchangeClient.field("PONG", "orderId"));
    }
}
