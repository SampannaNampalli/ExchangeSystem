package exchangeserver.gateway;

import exchangeserver.client.ExchangeClient;
import exchangeserver.core.ExchangeService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static exchangeserver.client.ExchangeClient.field;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// End to end: real sockets, real writer thread, real journal.
class TcpGatewayTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    private TcpGateway gateway;
    private ExchangeService service;
    private final List<ExchangeClient> clients = new ArrayList<>();

    @BeforeEach
    void start() throws IOException {
        gateway = new TcpGateway(0);
        service = ExchangeService.start(dir.resolve("gw.journal"), gateway);
        gateway.start(service);
    }

    @AfterEach
    void stop() throws Exception {
        for (ExchangeClient c : clients) {
            c.close();
        }
        gateway.close();
        assertTrue(service.stop(WAIT));
    }

    private ExchangeClient connect() throws IOException {
        ExchangeClient c = new ExchangeClient("localhost", gateway.port());
        clients.add(c);
        return c;
    }

    private ExchangeClient login(String participant) throws Exception {
        ExchangeClient c = connect();
        c.send("LOGIN " + participant);
        assertEquals("WELCOME participant=" + participant, c.awaitPrefix("WELCOME", WAIT));
        return c;
    }

    private List<String> journalLines() throws IOException {
        return Files.readAllLines(dir.resolve("gw.journal"), UTF_8);
    }

    @Nested
    class Session {

        @Test
        void mustLogInBeforeTrading() throws Exception {
            ExchangeClient c = connect();
            c.send("NEW c1 BUY LIMIT 1 100");
            assertEquals("ERROR LOGIN first", c.awaitPrefix("ERROR", WAIT));
            c.send("PING");
            assertEquals("PONG", c.awaitPrefix("PONG", WAIT), "PING works before LOGIN");
        }

        @Test
        void loginOnlyOnceAndOnlyValidNames() throws Exception {
            ExchangeClient c = connect();
            c.send("LOGIN bad,name");
            assertTrue(c.awaitPrefix("ERROR", WAIT).contains("invalid participant"));
            c.send("LOGIN alice");
            c.awaitPrefix("WELCOME", WAIT);
            c.send("LOGIN bob");
            assertTrue(c.awaitPrefix("ERROR", WAIT).contains("already logged in"));
        }

        @Test
        void quitSaysByeAndCloses() throws Exception {
            ExchangeClient c = login("alice");
            c.send("QUIT");
            assertEquals("BYE", c.awaitPrefix("BYE", WAIT));
            assertTrue(c.isClosedByServer(WAIT));
        }

        // D63: QUIT used to say BYE and close at once, dropping replies the writer had not produced yet.
        @Test
        void quitAnswersEveryEarlierRequestBeforeBye() throws Exception {
            ExchangeClient c = login("alice");
            int orders = 200;
            List<String> burst = new ArrayList<>();
            for (int i = 0; i < orders; i++) {
                burst.add("NEW q" + i + " BUY LIMIT 1 " + (100 + i % 10));
            }
            burst.add("QUIT");
            c.send(String.join("\n", burst));       // one write: the reader sees QUIT long before the writer is done

            List<String> lines = c.linesUntil(line -> line.equals("BYE"), WAIT);
            assertEquals(orders, lines.stream().filter(l -> l.startsWith("ORDER")).count(), lines.toString());
            assertEquals("BYE", lines.get(lines.size() - 1));
            assertTrue(c.isClosedByServer(WAIT));
        }

        @Test
        void endOfStreamStillGetsTheReplies() throws Exception {
            int orders = 50;
            StringBuilder burst = new StringBuilder("LOGIN bob\n");
            for (int i = 0; i < orders; i++) {
                burst.append("NEW e").append(i).append(" SELL LIMIT 1 200\n");
            }
            try (Socket raw = new Socket("localhost", gateway.port())) {
                raw.getOutputStream().write(burst.toString().getBytes(US_ASCII));
                raw.shutdownOutput();               // end of stream right after the last request
                raw.setSoTimeout((int) WAIT.toMillis());
                BufferedReader in = new BufferedReader(new InputStreamReader(raw.getInputStream(), US_ASCII));
                List<String> lines = new ArrayList<>();
                for (String line; (line = in.readLine()) != null; ) {
                    lines.add(line);
                }
                assertEquals(orders, lines.stream().filter(l -> l.startsWith("ORDER")).count(), lines.toString());
            }
        }

        @Test
        void malformedLineIsAnsweredAndTheSessionSurvives() throws Exception {
            ExchangeClient c = login("alice");
            c.send("NEW c1 BUY LIMIT ten 100");
            assertEquals("ERROR bad quantity: ten", c.awaitPrefix("ERROR", WAIT));
            c.send("PING");
            c.awaitPrefix("PONG", WAIT);
        }

        @Test
        void overlongLineDisconnects() throws Exception {
            ExchangeClient c = login("alice");
            c.send("PING " + "x".repeat(Protocol.MAX_LINE_LENGTH));
            assertTrue(c.awaitPrefix("ERROR", WAIT).contains("line longer than"));
            assertTrue(c.isClosedByServer(WAIT));
        }

        // Review finding 6: every participant name ever used used to leave an empty entry behind.
        @Test
        void participantRegistryForgetsDisconnectedParticipants() throws Exception {
            for (int i = 0; i < 20; i++) {
                ExchangeClient c = login("u" + i);
                c.send("QUIT");
                c.awaitPrefix("BYE", WAIT);
            }
            ExchangeClient stays = login("stays");
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (gateway.participantsRegistered() != 1 && System.nanoTime() < deadline) {
                Thread.onSpinWait();        // sessions unregister on their own threads; a concurrent map read is safe
            }
            assertEquals(1, gateway.participantsRegistered());
            stays.send("PING");
            stays.awaitPrefix("PONG", WAIT);
        }

        @Test
        void twoSessionsOfOneParticipantShareAnEntryUntilBothLeave() throws Exception {
            ExchangeClient a1 = login("alice");
            ExchangeClient a2 = login("alice");
            assertEquals(1, gateway.participantsRegistered());
            a1.send("QUIT");
            a1.awaitPrefix("BYE", WAIT);
            ExchangeClient bob = login("bob");
            // a2 must still receive its maker fill after a1 left.
            a2.send("NEW a1 SELL LIMIT 1 100");
            a2.awaitPrefix("ORDER", WAIT);
            bob.send("NEW b1 BUY LIMIT 1 100");
            assertTrue(a2.awaitPrefix("FILL", WAIT).contains("liquidity=MAKER"));
        }

        @Test
        void crlfLineEndingsAreAccepted() throws Exception {
            try (Socket raw = new Socket("localhost", gateway.port())) {
                OutputStream out = raw.getOutputStream();
                out.write("PING\r\n".getBytes(US_ASCII));
                out.flush();
                byte[] reply = new byte[5];
                assertEquals(5, raw.getInputStream().readNBytes(reply, 0, 5));
                assertEquals("PONG\n", new String(reply, US_ASCII));
            }
        }
    }

    @Nested
    class Trading {

        @Test
        void restThenCrossFillsBothSides() throws Exception {
            ExchangeClient alice = login("alice");
            ExchangeClient bob = login("bob");

            alice.send("NEW a1 SELL LIMIT 10 100");
            String rested = alice.awaitPrefix("ORDER", WAIT);
            assertEquals("ORDER orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10", rested);

            bob.send("NEW b1 BUY LIMIT 4 101");
            assertEquals("FILL orderId=2 clOrdId=b1 side=BUY price=100 qty=4 leaves=0 liquidity=TAKER", bob.awaitPrefix("FILL", WAIT));
            assertEquals("ORDER orderId=2 clOrdId=b1 status=FILLED filled=4 remaining=0", bob.awaitPrefix("ORDER", WAIT));
            assertEquals("FILL orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER", alice.awaitPrefix("FILL", WAIT));
        }

        @Test
        void takerHearsFillsBeforeTheOrderOutcome() throws Exception {
            ExchangeClient alice = login("alice");
            ExchangeClient bob = login("bob");
            alice.send("NEW a1 SELL LIMIT 1 100");
            alice.awaitPrefix("ORDER", WAIT);
            bob.send("NEW b1 BUY LIMIT 1 100");
            String first = bob.await(line -> true, WAIT);
            assertTrue(first.startsWith("FILL"), first);
        }

        @Test
        void everySessionOfTheMakerHearsItsFill() throws Exception {
            ExchangeClient alice1 = login("alice");
            ExchangeClient alice2 = login("alice");
            ExchangeClient bob = login("bob");
            alice1.send("NEW a1 SELL LIMIT 5 100");
            alice1.awaitPrefix("ORDER", WAIT);
            bob.send("NEW b1 BUY MARKET 5");
            assertTrue(alice1.awaitPrefix("FILL", WAIT).contains("liquidity=MAKER"));
            assertTrue(alice2.awaitPrefix("FILL", WAIT).contains("liquidity=MAKER"));
        }

        @Test
        void cancelOwnOrderButNotSomeoneElses() throws Exception {
            ExchangeClient alice = login("alice");
            ExchangeClient mallory = login("mallory");
            alice.send("NEW a1 BUY LIMIT 10 99");
            String id = field(alice.awaitPrefix("ORDER", WAIT), "orderId");

            mallory.send("CANCEL " + id);
            assertEquals("CANCEL_REJECT orderId=" + id + " reason=UNKNOWN_ORDER", mallory.awaitPrefix("CANCEL", WAIT));
            alice.send("CANCEL " + id);
            assertEquals("CANCELLED orderId=" + id + " qty=10", alice.awaitPrefix("CANCEL", WAIT));
        }

        @Test
        void duplicateClientOrderIdIsRejected() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW same BUY LIMIT 1 99");
            alice.awaitPrefix("ORDER", WAIT);
            alice.send("NEW same BUY LIMIT 1 98");
            String line = alice.awaitPrefix("ORDER", WAIT);
            assertEquals("REJECTED", field(line, "status"));
            assertEquals("DUPLICATE_CLIENT_ORDER_ID", field(line, "reason"));
            assertEquals("1", field(line, "orderId"));
        }

        @Test
        void statelessRejectsNeverReachTheJournal() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW c1 BUY LIMIT 0 100");
            assertTrue(alice.awaitPrefix("ERROR", WAIT).startsWith("ERROR clOrdId=c1 Quantity must be positive"));
            alice.send("NEW c2 BUY LIMIT 1 -5");
            alice.awaitPrefix("ERROR", WAIT);
            alice.send("NEW bad,id BUY LIMIT 1 100");
            alice.awaitPrefix("ERROR", WAIT);
            alice.send("CANCEL 0");
            alice.awaitPrefix("ERROR", WAIT);
            alice.send("NEW ok BUY LIMIT 1 100");
            alice.awaitPrefix("ORDER", WAIT);
            assertEquals(2, journalLines().size(), "header + the one valid order");
        }

        @Test
        void afterStopRequestsAreRefused() throws Exception {
            ExchangeClient alice = login("alice");
            assertTrue(service.stop(WAIT));
            alice.send("NEW c1 BUY LIMIT 1 100");
            assertEquals("ERROR clOrdId=c1 STOPPED", alice.awaitPrefix("ERROR", WAIT));
        }
    }

    @Nested
    class MarketData {

        @Test
        void snapshotThenIncrementalUpdates() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW a1 SELL LIMIT 10 100");
            alice.awaitPrefix("ORDER", WAIT);

            ExchangeClient watcher = login("carol");
            watcher.send("SUBSCRIBE");
            assertEquals("SUBSCRIBED", watcher.awaitPrefix("SUBSCRIBED", WAIT));
            assertEquals("BOOK seq=1 last=0 bids=- asks=100x10", watcher.awaitPrefix("BOOK", WAIT));

            ExchangeClient bob = login("bob");
            bob.send("NEW b1 BUY LIMIT 4 100");
            assertEquals("TRADE seq=2 price=100 qty=4 aggressor=BUY", watcher.awaitPrefix("TRADE", WAIT));
            assertEquals("LEVEL seq=2 side=SELL price=100 qty=6 orders=1", watcher.awaitPrefix("LEVEL", WAIT));

            alice.send("CANCEL 1");
            assertEquals("LEVEL seq=3 side=SELL price=100 qty=0 orders=0", watcher.awaitPrefix("LEVEL", WAIT));
        }

        @Test
        void subscribeTwiceIsAnErrorAndUnsubscribeStopsUpdates() throws Exception {
            ExchangeClient watcher = login("carol");
            watcher.send("SUBSCRIBE");
            watcher.awaitPrefix("BOOK", WAIT);
            watcher.send("SUBSCRIBE");
            assertEquals("ERROR already subscribed", watcher.awaitPrefix("ERROR", WAIT));
            watcher.send("UNSUBSCRIBE");
            watcher.awaitPrefix("UNSUBSCRIBED", WAIT);

            ExchangeClient alice = login("alice");
            alice.send("NEW a1 SELL LIMIT 10 100");
            alice.awaitPrefix("ORDER", WAIT);
            // SUBSCRIBE goes through the writer queue behind alice's order, so any market data for that order
            // would reach the watcher before SUBSCRIBED. (PING would not prove it: the reader thread answers it.)
            watcher.send("SUBSCRIBE");
            assertEquals(List.of("SUBSCRIBED"), watcher.linesUntil(line -> line.equals("SUBSCRIBED"), WAIT));
        }

        // Review finding 3: an UNSUBSCRIBE overtaking the snapshot used to leave the client's last word as
        // SUBSCRIBED while it was not subscribed. Whatever the interleaving, the last status line must match the
        // real state, and at most one subscription may exist.
        @Test
        void rapidSubscribeUnsubscribeEndsConsistent() throws Exception {
            ExchangeClient watcher = login("carol");
            ExchangeClient alice = login("alice");
            for (int round = 0; round < 20; round++) {
                for (int i = 0; i < 5; i++) {
                    watcher.send("SUBSCRIBE");
                    watcher.send("UNSUBSCRIBE");
                }
                boolean endSubscribed = round % 2 == 0;
                if (endSubscribed) {
                    watcher.send("SUBSCRIBE");
                }
                watcher.send("PING");
                List<String> lines = watcher.linesUntil(line -> line.equals("PONG"), WAIT);
                // The reader has handled every line; an order through the writer flushes any pending snapshot.
                alice.send("NEW r" + round + " BUY LIMIT 1 " + (90 + round % 5));
                alice.awaitPrefix("ORDER", WAIT);
                watcher.send("PING");
                lines.addAll(watcher.linesUntil(line -> line.equals("PONG"), WAIT));

                String last = lines.stream().filter(l -> l.equals("SUBSCRIBED") || l.equals("UNSUBSCRIBED"))
                        .reduce((a, b) -> b).orElse("none");
                assertEquals(endSubscribed ? "SUBSCRIBED" : "UNSUBSCRIBED", last, lines.toString());
                assertEquals(endSubscribed ? 1 : 0, gateway.subscriberCount(), lines.toString());
                if (endSubscribed) {
                    watcher.send("UNSUBSCRIBE");
                    watcher.awaitPrefix("UNSUBSCRIBED", WAIT);
                }
            }
        }

        // Review finding 7: SUBSCRIBE used to snapshot the whole book on the writer thread.
        @Test
        void snapshotIsBoundedToTheSnapshotDepth() throws Exception {
            ExchangeClient alice = login("alice");
            for (int i = 0; i < Protocol.SNAPSHOT_DEPTH + 20; i++) {
                alice.send("NEW c" + i + " BUY LIMIT 1 " + (1000 + i));
            }
            for (int i = 0; i < Protocol.SNAPSHOT_DEPTH + 20; i++) {
                alice.awaitPrefix("ORDER", WAIT);
            }
            ExchangeClient watcher = login("carol");
            watcher.send("SUBSCRIBE");
            String book = watcher.awaitPrefix("BOOK", WAIT);
            String bids = field(book, "bids");
            assertEquals(Protocol.SNAPSHOT_DEPTH, bids.split(",").length);
            assertTrue(bids.startsWith((1000 + Protocol.SNAPSHOT_DEPTH + 19) + "x1"), "best levels first");
        }

        @Test
        void nonSubscribersGetNoMarketData() throws Exception {
            ExchangeClient alice = login("alice");
            ExchangeClient bob = login("bob");
            alice.send("NEW a1 SELL LIMIT 1 100");
            alice.awaitPrefix("ORDER", WAIT);
            bob.send("NEW b1 BUY LIMIT 1 100");
            bob.send("SUBSCRIBE");
            List<String> lines = bob.linesUntil(line -> line.equals("SUBSCRIBED"), WAIT);
            assertEquals(3, lines.size(), lines.toString());
            assertTrue(lines.get(0).startsWith("FILL"));
            assertTrue(lines.get(1).startsWith("ORDER"));
        }
    }

    @Test
    void restartKeepsTheBookForTheNextGateway() throws Exception {
        ExchangeClient alice = login("alice");
        alice.send("NEW a1 SELL LIMIT 10 100");
        alice.awaitPrefix("ORDER", WAIT);
        gateway.close();
        assertTrue(service.stop(WAIT));

        gateway = new TcpGateway(0);
        service = ExchangeService.start(dir.resolve("gw.journal"), gateway);
        gateway.start(service);
        ExchangeClient carol = login("carol");
        carol.send("SUBSCRIBE");
        assertEquals("BOOK seq=1 last=0 bids=- asks=100x10", carol.awaitPrefix("BOOK", WAIT));
    }
}
