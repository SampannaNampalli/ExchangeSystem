package exchangeserver.gateway;

import exchangeserver.client.ExchangeClient;
import exchangeserver.core.Exchange;
import exchangeserver.core.ExchangeService;
import exchangeserver.core.OutputListener;
import exchangeserver.core.PrivateEvent;
import exchangeserver.engine.EngineOutput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static exchangeserver.client.ExchangeClient.field;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// End to end: real sockets, real writer thread, real journal.
class TcpGatewayTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final Credentials CREDENTIALS = credentials();

    @TempDir
    Path dir;

    private TcpGateway gateway;
    private ExchangeService service;
    private final List<ExchangeClient> clients = new ArrayList<>();

    private static Credentials credentials() {
        Map<String, String> secrets = new HashMap<>();
        for (String p : List.of("alice", "bob", "carol", "mallory", "stays", "dave")) {
            secrets.put(p, secret(p));
        }
        for (int i = 0; i < 20; i++) {
            secrets.put("u" + i, secret("u" + i));
        }
        return Credentials.of(secrets, 1_000);          // few iterations: tests log in hundreds of times
    }

    private static String secret(String participant) {
        return "secret-" + participant;
    }

    // Holds the writer thread once, right after the output of the next command once armed (a stand-in for a slow
    // command, so requests stay in flight).
    private static final class Hold implements OutputListener {
        volatile OutputListener next = NONE;
        volatile boolean armed;
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void onOutput(EngineOutput output, List<PrivateEvent> events) {
            next.onOutput(output, events);
            if (armed) {
                armed = false;
                reached.countDown();
                try {
                    release.await(WAIT.toSeconds(), TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @BeforeEach
    void start() throws IOException {
        start(TcpGateway.Settings.defaults(0, CREDENTIALS), ExchangeService.Settings.defaults(), null);
    }

    private void start(TcpGateway.Settings gatewaySettings, ExchangeService.Settings serviceSettings, Hold hold)
            throws IOException {
        gateway = new TcpGateway(gatewaySettings);
        OutputListener listener = gateway;
        if (hold != null) {
            hold.next = gateway;
            listener = hold;
        }
        service = ExchangeService.start(dir.resolve("gw.journal"), listener, serviceSettings);
        gateway.start(service);
    }

    private void restart(TcpGateway.Settings gatewaySettings, ExchangeService.Settings serviceSettings, Hold hold)
            throws Exception {
        gateway.close();
        assertTrue(service.stop(WAIT));
        start(gatewaySettings, serviceSettings, hold);
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
        return login(participant, "");
    }

    private ExchangeClient login(String participant, String flags) throws Exception {
        ExchangeClient c = connect();
        c.send("LOGIN " + participant + " " + secret(participant) + flags);
        assertTrue(c.awaitPrefix("WELCOME", WAIT).startsWith("WELCOME participant=" + participant + " oseq="));
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
        void welcomeReportsTheLatestOseq() throws Exception {
            ExchangeClient c = connect();
            c.send("LOGIN alice " + secret("alice"));
            assertEquals("WELCOME participant=alice oseq=0", c.awaitPrefix("WELCOME", WAIT));
            c.send("LOGIN alice " + secret("alice"));
            assertTrue(c.awaitPrefix("ERROR", WAIT).contains("already logged in"));
        }

        @Test
        void quitSaysByeAndCloses() throws Exception {
            ExchangeClient c = login("alice");
            c.send("QUIT");
            assertEquals("BYE", c.awaitPrefix("BYE", WAIT));
            assertTrue(c.isClosedByServer(WAIT));
        }

        // D63/D76: QUIT used to say BYE and close at once, dropping replies the writer had not produced yet.
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
            StringBuilder burst = new StringBuilder("LOGIN bob " + secret("bob") + "\n");
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

        // D76 (M10): the D63 barrier was refused once the exchange was stopping, so a session closed at once and
        // lost the replies of requests accepted before the stop. The in-flight count waits for them.
        @Test
        void quitDuringShutdownStillGetsTheRepliesOfAcceptedRequests() throws Exception {
            Hold hold = new Hold();
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS), ExchangeService.Settings.defaults(), hold);
            ExchangeClient bob = login("bob");
            ExchangeClient alice = login("alice");
            hold.armed = true;
            bob.send("NEW b1 BUY LIMIT 1 90");                  // the writer stops right after this one
            assertTrue(hold.reached.await(WAIT.toSeconds(), TimeUnit.SECONDS));
            alice.send("NEW a1 SELL LIMIT 1 110");              // accepted, queued behind the held writer
            alice.send("PING");
            alice.awaitPrefix("PONG", WAIT);                    // the reader has handed a1 in

            Thread stopper = new Thread(() -> {
                try {
                    service.stop(WAIT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            stopper.start();
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (service.state() == ExchangeService.State.RUNNING && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertEquals(ExchangeService.State.STOPPING, service.state());
            alice.send("QUIT");
            deadline = System.nanoTime() + WAIT.toNanos();
            while (gateway.drainingSessions() != 1 && System.nanoTime() < deadline) {
                Thread.onSpinWait();                            // QUIT read, a1 still in flight
            }
            assertEquals(1, gateway.drainingSessions());
            hold.release.countDown();

            List<String> lines = alice.linesUntil(line -> line.equals("BYE"), WAIT);
            assertTrue(lines.stream().anyMatch(l -> l.startsWith("ORDER") && l.contains("clOrdId=a1")), lines.toString());
            stopper.join(WAIT.toMillis());
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

        // L6: a client that connects while the journal is still being replayed gets an answer, not silence.
        @Test
        void beforeTheExchangeIsAttachedSessionsAreToldItIsStarting() throws Exception {
            try (TcpGateway early = new TcpGateway(TcpGateway.Settings.defaults(0, CREDENTIALS));
                 ExchangeClient c = new ExchangeClient("localhost", early.port())) {
                early.startAccepting();
                c.send("LOGIN alice " + secret("alice"));
                assertTrue(c.awaitPrefix("ERROR", WAIT).contains("starting"));
                assertTrue(c.isClosedByServer(WAIT));
            }
        }
    }

    // D74 (H1): only the holder of a participant's secret acts as that participant.
    @Nested
    class Security {

        @Test
        void bindsToTheLoopbackAddressByDefault() {
            assertTrue(TcpGateway.Settings.defaults(0, CREDENTIALS).bindAddress().isLoopbackAddress());
        }

        @Test
        void aWrongSecretIsRefusedAndDisconnects() throws Exception {
            ExchangeClient c = connect();
            c.send("LOGIN alice not-her-secret");
            assertEquals("ERROR login failed", c.awaitPrefix("ERROR", WAIT));
            assertTrue(c.isClosedByServer(WAIT), "one attempt per connection");
        }

        @Test
        void anUnknownOrInvalidNameGetsTheSameAnswer() throws Exception {
            for (String line : List.of("LOGIN nobody secret-nobody", "LOGIN bad,name whatever-secret")) {
                ExchangeClient c = connect();
                c.send(line);
                assertEquals("ERROR login failed", c.awaitPrefix("ERROR", WAIT), line);
                assertTrue(c.isClosedByServer(WAIT));
            }
        }

        @Test
        void credentialsFileRoundTripAndVerify() throws Exception {
            Path users = dir.resolve("exchange.users");
            Credentials.addUser(users, "alice", "first-secret", 1_000);
            Credentials.addUser(users, "bob", "bobs-secret", 1_000);
            Credentials.addUser(users, "alice", "second-secret", 1_000);       // replaces
            Credentials loaded = Credentials.load(users);
            assertEquals(2, loaded.size());
            assertTrue(loaded.verify("alice", "second-secret"));
            assertFalse(loaded.verify("alice", "first-secret"));
            assertFalse(loaded.verify("carol", "second-secret"));
            assertFalse(Files.readString(users).contains("second-secret"), "the secret itself is never stored");
        }

        @Test
        void tlsRoundTrip() throws Exception {
            Path keystore = dir.resolve("server.p12");
            Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
            Process p = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "es2", "-keyalg", "EC",
                    "-groupname", "secp256r1", "-dname", "CN=localhost", "-validity", "2", "-keystore", keystore.toString(),
                    "-storetype", "PKCS12", "-storepass", "changeit", "-keypass", "changeit")
                    .redirectErrorStream(true).start();
            assertTrue(p.waitFor(60, TimeUnit.SECONDS));
            assertEquals(0, p.exitValue(), new String(p.getInputStream().readAllBytes(), UTF_8));

            SSLContext server = Tls.serverContext(keystore, "changeit".toCharArray());
            SSLContext client = Tls.clientContext(keystore, "changeit".toCharArray());
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS).withTls(server), ExchangeService.Settings.defaults(), null);
            ExchangeClient c = new ExchangeClient("localhost", gateway.port(), client.getSocketFactory());
            clients.add(c);
            c.send("LOGIN alice " + secret("alice"));
            assertEquals("WELCOME participant=alice oseq=0", c.awaitPrefix("WELCOME", WAIT));

            try (Socket plain = new Socket(InetAddress.getLoopbackAddress(), gateway.port())) {
                plain.setSoTimeout((int) WAIT.toMillis());
                plain.getOutputStream().write("PING\n".getBytes(US_ASCII));
                // The server expects a TLS handshake and never answers plain text with PONG.
                byte[] reply = plain.getInputStream().readNBytes(4);
                assertFalse(new String(reply, US_ASCII).equals("PONG"));
            } catch (IOException expected) {
                // the server dropped the connection: also fine
            }
        }
    }

    // D75 (M1, M2, H2): one client cannot hold slots forever or crowd the others out.
    @Nested
    class Limits {

        @Test
        void aConnectionThatNeverLogsInIsDisconnected() throws Exception {
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS).withTimeouts(Duration.ofMillis(300), Duration.ofSeconds(30)),
                    ExchangeService.Settings.defaults(), null);
            ExchangeClient c = connect();
            assertEquals("ERROR login timeout; disconnecting", c.awaitPrefix("ERROR", WAIT));
            assertTrue(c.isClosedByServer(WAIT));
        }

        @Test
        void anIdleSessionIsDisconnected() throws Exception {
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS).withTimeouts(Duration.ofSeconds(30), Duration.ofMillis(300)),
                    ExchangeService.Settings.defaults(), null);
            ExchangeClient c = login("alice");
            assertEquals("ERROR idle timeout; disconnecting", c.awaitPrefix("ERROR", WAIT));
            assertTrue(c.isClosedByServer(WAIT));
        }

        @Test
        void requestsBeyondTheRateAreRefusedWithoutReachingTheJournal() throws Exception {
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS).withFlowControl(1_000, 0.001, 2),
                    ExchangeService.Settings.defaults(), null);
            ExchangeClient c = login("alice");
            c.send("NEW c1 BUY LIMIT 1 100");
            c.send("NEW c2 BUY LIMIT 1 100");
            c.send("NEW c3 BUY LIMIT 1 100");
            assertEquals("ERROR clOrdId=c3 RATE_LIMITED", c.awaitPrefix("ERROR", WAIT));
            c.awaitPrefix("ORDER", WAIT);
            c.awaitPrefix("ORDER", WAIT);
            assertEquals(3, journalLines().size(), "header + the two admitted orders");
        }

        @Test
        void requestsBeyondTheInFlightLimitAreRefused() throws Exception {
            Hold hold = new Hold();
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS).withFlowControl(2, 1_000, 1_000),
                    ExchangeService.Settings.defaults(), hold);
            ExchangeClient c = login("alice");
            hold.armed = true;
            c.send("NEW c1 BUY LIMIT 1 100");               // the writer stops after it: c1 stays in flight
            assertTrue(hold.reached.await(WAIT.toSeconds(), TimeUnit.SECONDS));
            c.send("NEW c2 BUY LIMIT 1 100");
            c.send("NEW c3 BUY LIMIT 1 100");
            assertEquals("ERROR clOrdId=c3 SESSION_BUSY", c.awaitPrefix("ERROR", WAIT));
            hold.release.countDown();
            c.awaitPrefix("ORDER", WAIT);
            c.awaitPrefix("ORDER", WAIT);
        }
    }

    @Nested
    class Trading {

        @Test
        void restThenCrossFillsBothSides() throws Exception {
            ExchangeClient alice = login("alice");
            ExchangeClient bob = login("bob");

            alice.send("NEW a1 SELL LIMIT 10 100");
            assertEquals("ORDER oseq=1 orderId=1 clOrdId=a1 status=RESTED filled=0 remaining=10", alice.awaitPrefix("ORDER", WAIT));

            bob.send("NEW b1 BUY LIMIT 4 101");
            assertEquals("FILL oseq=1 orderId=2 clOrdId=b1 side=BUY price=100 qty=4 leaves=0 liquidity=TAKER", bob.awaitPrefix("FILL", WAIT));
            assertEquals("ORDER oseq=1 orderId=2 clOrdId=b1 status=FILLED filled=4 remaining=0", bob.awaitPrefix("ORDER", WAIT));
            assertEquals("FILL oseq=2 orderId=1 clOrdId=a1 side=SELL price=100 qty=4 leaves=6 liquidity=MAKER", alice.awaitPrefix("FILL", WAIT));
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

        // D73: the private stream goes to every session of the participant, in oseq order.
        @Test
        void everySessionOfAParticipantSeesItsWholeStream() throws Exception {
            ExchangeClient alice1 = login("alice");
            ExchangeClient alice2 = login("alice");
            ExchangeClient bob = login("bob");
            alice1.send("NEW a1 SELL LIMIT 5 100");
            assertEquals(alice1.awaitPrefix("ORDER", WAIT), alice2.awaitPrefix("ORDER", WAIT));
            bob.send("NEW b1 BUY MARKET 5");
            assertTrue(alice1.awaitPrefix("FILL", WAIT).contains("oseq=2"));
            assertTrue(alice2.awaitPrefix("FILL", WAIT).contains("liquidity=MAKER"));
        }

        @Test
        void cancelOwnOrderButNotSomeoneElses() throws Exception {
            ExchangeClient alice = login("alice");
            ExchangeClient mallory = login("mallory");
            alice.send("NEW a1 BUY LIMIT 10 99");
            String id = field(alice.awaitPrefix("ORDER", WAIT), "orderId");

            mallory.send("CANCEL " + id);
            assertEquals("CANCEL_REJECT oseq=1 orderId=" + id + " reason=UNKNOWN_ORDER", mallory.awaitPrefix("CANCEL", WAIT));
            alice.send("CANCEL " + id);
            assertEquals("CANCELLED oseq=2 orderId=" + id + " qty=10", alice.awaitPrefix("CANCEL", WAIT));
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
        void selfTradeIsPreventedOverTheWire() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW a1 SELL LIMIT 5 100");
            alice.awaitPrefix("ORDER", WAIT);
            alice.send("NEW a2 BUY LIMIT 5 100");
            assertEquals("ORDER oseq=2 orderId=2 clOrdId=a2 status=CANCELLED filled=0 remaining=5 reason=SELF_TRADE_PREVENTED",
                    alice.awaitPrefix("ORDER", WAIT));
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
            alice.send("RESEND 0");
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

    // D73 (M3): a client that was away recovers what it missed.
    @Nested
    class Resend {

        @Test
        void aReconnectingClientRecoversAMakerFillItMissed() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW a1 SELL LIMIT 5 100");
            assertTrue(alice.awaitPrefix("ORDER", WAIT).startsWith("ORDER oseq=1 "));
            alice.send("QUIT");
            alice.awaitPrefix("BYE", WAIT);

            ExchangeClient bob = login("bob");
            bob.send("NEW b1 BUY LIMIT 2 100");
            bob.awaitPrefix("ORDER", WAIT);

            ExchangeClient again = connect();
            again.send("LOGIN alice " + secret("alice"));
            assertEquals("WELCOME participant=alice oseq=2", again.awaitPrefix("WELCOME", WAIT));
            again.send("RESEND 2");
            assertEquals(List.of(
                    "FILL oseq=2 orderId=1 clOrdId=a1 side=SELL price=100 qty=2 leaves=3 liquidity=MAKER resend=1",
                    "RESENT last=2"), again.linesUntil(line -> line.startsWith("RESENT"), WAIT));
        }

        @Test
        void eventsTooOldToRetainAreReportedAsAGap() throws Exception {
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS),
                    ExchangeService.Settings.defaults().withExchangeOptions(new Exchange.Options(10_000, 2)), null);
            ExchangeClient alice = login("alice");
            for (int i = 1; i <= 4; i++) {
                alice.send("NEW c" + i + " BUY LIMIT 1 " + (90 + i));
                alice.awaitPrefix("ORDER", WAIT);
            }
            alice.send("RESEND 1");
            List<String> lines = alice.linesUntil(line -> line.startsWith("RESENT"), WAIT);
            assertEquals("ERROR RESEND_GAP first=3", lines.get(0));
            assertTrue(lines.get(1).startsWith("ORDER oseq=3 ") && lines.get(1).endsWith("resend=1"), lines.toString());
            assertEquals("RESENT last=4", lines.get(lines.size() - 1));
        }

        @Test
        void theStreamSurvivesARestart() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW a1 SELL LIMIT 5 100");
            alice.awaitPrefix("ORDER", WAIT);
            restart(TcpGateway.Settings.defaults(0, CREDENTIALS), ExchangeService.Settings.defaults(), null);
            ExchangeClient again = connect();
            again.send("LOGIN alice " + secret("alice"));
            assertEquals("WELCOME participant=alice oseq=1", again.awaitPrefix("WELCOME", WAIT), "rebuilt by replay");
            again.send("RESEND 1");
            assertTrue(again.awaitPrefix("ORDER", WAIT).endsWith("resend=1"));
        }
    }

    // D72 (M8): a participant can ask for its orders to be cancelled when its last session closes.
    @Nested
    class CancelOnDisconnect {

        @Test
        void theLastCodSessionClosingCancelsTheParticipantsOrders() throws Exception {
            ExchangeClient watcher = login("carol");
            watcher.send("SUBSCRIBE");
            watcher.awaitPrefix("BOOK", WAIT);
            ExchangeClient alice = login("alice", " COD");
            alice.send("NEW a1 SELL LIMIT 5 100");
            alice.send("NEW a2 SELL LIMIT 5 101");
            alice.awaitPrefix("ORDER", WAIT);
            alice.awaitPrefix("ORDER", WAIT);
            alice.send("QUIT");
            alice.awaitPrefix("BYE", WAIT);

            assertEquals("LEVEL seq=3 side=SELL price=100 qty=0 orders=0",
                    watcher.await(l -> l.startsWith("LEVEL") && l.contains("qty=0") && l.contains("price=100"), WAIT));
            ExchangeClient again = connect();
            again.send("LOGIN alice " + secret("alice"));
            assertEquals("WELCOME participant=alice oseq=4", again.awaitPrefix("WELCOME", WAIT));
            again.send("RESEND 3");
            assertEquals("CANCELLED oseq=3 orderId=1 qty=5 reason=CANCEL_ALL resend=1", again.awaitPrefix("CANCELLED", WAIT));
        }

        @Test
        void withoutCodOrdersStay() throws Exception {
            ExchangeClient alice = login("alice");
            alice.send("NEW a1 SELL LIMIT 5 100");
            alice.awaitPrefix("ORDER", WAIT);
            alice.send("QUIT");
            alice.awaitPrefix("BYE", WAIT);
            assertTrue(alice.isClosedByServer(WAIT));
            assertEquals(1, service.snapshot(1).get(WAIT.toSeconds(), TimeUnit.SECONDS).asks().size());
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
        restart(TcpGateway.Settings.defaults(0, CREDENTIALS), ExchangeService.Settings.defaults(), null);
        ExchangeClient carol = login("carol");
        carol.send("SUBSCRIBE");
        assertEquals("BOOK seq=1 last=0 bids=- asks=100x10", carol.awaitPrefix("BOOK", WAIT));
    }
}
