package exchangeserver.gateway;

import exchangeserver.book.LevelUpdate;
import exchangeserver.core.ExchangeService;
import exchangeserver.core.OutputListener;
import exchangeserver.core.PrivateEvent;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.Trade;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.System.Logger.Level;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.US_ASCII;

/*

The TCP gateway (roadmap step 4B). Plain TCP or TLS, a line protocol (Protocol), thread-per-connection.

Threads: one acceptor; per connection a reader and a sender (ClientSession). The exchange's writer thread calls
onOutput() and the reply callbacks; both only offer lines to session queues, so no client can slow the exchange.

Security (D74): binds to the loopback address unless told otherwise; every session logs in with a secret checked
against Credentials; TLS when an SSLContext is given. Limits per session (D75): login and idle timeouts, a cap on
requests in flight, a request rate limit.

Routing:
    private events (replies, fills)  every session logged in as that participant, in oseq order (D73)
    market data (TRADE, LEVEL)       every subscribed session
    a refusal (BUSY, RATE_LIMITED)   the session that sent the request

Usage: create (binds the port), startAccepting() (sessions answer "starting" until attach), pass to
ExchangeService.start as its OutputListener, then attach(service). start(service) does both steps at once.

 */
public final class TcpGateway implements OutputListener, AutoCloseable {

    public record Settings(InetAddress bindAddress, int port, Credentials credentials, SSLContext tls, int maxSessions,
                           int outboundCapacity, Duration loginTimeout, Duration idleTimeout, int maxInFlight,
                           double ratePerSecond, int burst) {

        public Settings {
            Objects.requireNonNull(bindAddress, "bindAddress");
            Objects.requireNonNull(credentials, "credentials");
            Objects.requireNonNull(loginTimeout, "loginTimeout");
            Objects.requireNonNull(idleTimeout, "idleTimeout");
            if (port < 0 || maxSessions <= 0 || outboundCapacity <= 0 || maxInFlight <= 0 || ratePerSecond <= 0
                    || burst <= 0 || loginTimeout.isNegative() || loginTimeout.isZero() || idleTimeout.isNegative()
                    || idleTimeout.isZero()) {
                throw new IllegalArgumentException("Bad gateway settings");
            }
        }

        public static Settings defaults(int port, Credentials credentials) {
            return new Settings(InetAddress.getLoopbackAddress(), port, credentials, null, 256, 10_000,
                    Duration.ofSeconds(10), Duration.ofMinutes(5), 1_000, 1_000, 2_000);
        }

        public Settings withBindAddress(InetAddress address) {
            return new Settings(address, port, credentials, tls, maxSessions, outboundCapacity, loginTimeout, idleTimeout,
                    maxInFlight, ratePerSecond, burst);
        }

        public Settings withTls(SSLContext context) {
            return new Settings(bindAddress, port, credentials, context, maxSessions, outboundCapacity, loginTimeout,
                    idleTimeout, maxInFlight, ratePerSecond, burst);
        }

        public Settings withLimits(int sessions, int outbound) {
            return new Settings(bindAddress, port, credentials, tls, sessions, outbound, loginTimeout, idleTimeout,
                    maxInFlight, ratePerSecond, burst);
        }

        public Settings withTimeouts(Duration login, Duration idle) {
            return new Settings(bindAddress, port, credentials, tls, maxSessions, outboundCapacity, login, idle,
                    maxInFlight, ratePerSecond, burst);
        }

        public Settings withFlowControl(int inFlight, double perSecond, int burstSize) {
            return new Settings(bindAddress, port, credentials, tls, maxSessions, outboundCapacity, loginTimeout,
                    idleTimeout, inFlight, perSecond, burstSize);
        }
    }

    private static final System.Logger LOG = System.getLogger(TcpGateway.class.getName());
    private static final long MIN_BACKOFF_MILLIS = 100;
    private static final long MAX_BACKOFF_MILLIS = 5_000;
    private static final int ALARM_AFTER_FAILURES = 10;

    private final Settings settings;
    private final ServerSocket server;
    private final AtomicLong sessionIds = new AtomicLong();
    private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();
    private final Set<ClientSession> subscribers = ConcurrentHashMap.newKeySet();
    private final Map<String, Set<ClientSession>> sessionsByParticipant = new ConcurrentHashMap<>();
    private volatile ExchangeService service;
    private Thread acceptor;

    public TcpGateway(Settings settings) throws IOException {
        this.settings = Objects.requireNonNull(settings, "settings");
        if (settings.tls() == null) {
            this.server = new ServerSocket();
        } else {
            SSLServerSocket tls = (SSLServerSocket) settings.tls().getServerSocketFactory().createServerSocket();
            tls.setEnabledProtocols(Tls.PROTOCOLS);
            this.server = tls;
        }
        try {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(settings.bindAddress(), settings.port()));
        } catch (IOException e) {
            server.close();
            throw e;
        }
    }

    public int port() {
        return server.getLocalPort();
    }

    Settings settings() {
        return settings;
    }

    // Accepts connections from now on; until attach(), sessions are told the exchange is starting (L6).
    public synchronized void startAccepting() {
        if (acceptor != null) {
            throw new IllegalStateException("Already accepting");
        }
        acceptor = new Thread(this::acceptLoop, "gateway-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public void attach(ExchangeService exchangeService) {
        this.service = Objects.requireNonNull(exchangeService, "exchangeService");
    }

    public synchronized void start(ExchangeService exchangeService) {
        attach(exchangeService);
        if (acceptor == null) {
            startAccepting();
        }
    }

    ExchangeService service() {
        return service;
    }

    private void acceptLoop() {
        int failures = 0;
        while (!server.isClosed()) {
            Socket socket;
            try {
                socket = server.accept();
                failures = 0;
            } catch (IOException e) {
                // Only a closed server socket ends the loop. Anything else (e.g. "Too many open files", also a
                // SocketException) is passing: back off, keep accepting, and alarm if it persists (L6).
                if (server.isClosed()) {
                    return;
                }
                failures++;
                LOG.log(failures >= ALARM_AFTER_FAILURES ? Level.ERROR : Level.WARNING,
                        "accept failed " + failures + " time(s) in a row; retrying", e);
                try {
                    Thread.sleep(Math.min(MAX_BACKOFF_MILLIS, MIN_BACKOFF_MILLIS << Math.min(failures - 1, 6)));
                } catch (InterruptedException interrupted) {
                    return;
                }
                continue;
            }
            try {
                socket.setTcpNoDelay(true);
                if (sessions.size() >= settings.maxSessions()) {
                    socket.setSoTimeout(2_000);                 // a TLS handshake here must not stall the acceptor
                    OutputStream out = socket.getOutputStream();
                    out.write((Protocol.error("too many sessions") + "\n").getBytes(US_ASCII));
                    out.flush();
                    closeQuietly(socket);
                    continue;
                }
            } catch (IOException e) {
                closeQuietly(socket);           // a client that reset at once must not leak a descriptor
                continue;
            }
            ClientSession session = new ClientSession(sessionIds.incrementAndGet(), socket, this);
            sessions.add(session);
            session.start();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing left to do
        }
    }

    // ---------- Called by sessions ----------

    boolean authenticate(String participant, String secret) {
        return settings.credentials().verify(participant, secret);
    }

    // Add and remove both run inside compute*, which is atomic per key: a remove that drops an emptied set cannot
    // interleave with an add into that same set (the add would land in a set no longer in the map).
    void loggedIn(ClientSession session) {
        sessionsByParticipant.compute(session.participant(), (p, mine) -> {
            Set<ClientSession> set = mine == null ? ConcurrentHashMap.newKeySet() : mine;
            set.add(session);
            return set;
        });
    }

    void subscribe(ClientSession session) {
        subscribers.add(session);
    }

    void unsubscribe(ClientSession session) {
        subscribers.remove(session);
    }

    void removed(ClientSession session) {
        sessions.remove(session);
        subscribers.remove(session);
        String p = session.participant();
        if (p == null) {
            return;
        }
        // Drop the participant's entry with its last session, so ever-new names cannot grow the map.
        boolean[] wasLast = {false};
        sessionsByParticipant.computeIfPresent(p, (k, mine) -> {
            mine.remove(session);
            wasLast[0] = mine.isEmpty();
            return mine.isEmpty() ? null : mine;
        });
        ExchangeService s = service;
        if (wasLast[0] && session.cancelOnDisconnect() && s != null) {
            // D72: the participant's last session is gone and it asked for this. A journalled CANCEL_ALL, so the
            // replies wait in its private stream for RESEND.
            s.cancelAll(p, (results, failure) -> {
                if (failure != null) {
                    LOG.log(Level.WARNING, "cancel-on-disconnect for " + p + " failed: " + failure);
                }
            });
        }
    }

    // For tests: participants with at least one registered session.
    int participantsRegistered() {
        return sessionsByParticipant.size();
    }

    int subscriberCount() {
        return subscribers.size();
    }

    // For tests: sessions whose reader has stopped and that wait for their last replies (D76).
    int drainingSessions() {
        return (int) sessions.stream().filter(ClientSession::draining).count();
    }

    void log(String message) {
        LOG.log(Level.INFO, message);
    }

    // ---------- Writer thread ----------

    @Override
    public void onOutput(EngineOutput output, List<PrivateEvent> events) {
        for (PrivateEvent e : events) {
            Set<ClientSession> owners = sessionsByParticipant.get(e.participant());
            if (owners != null) {
                List<String> lines = Protocol.lines(e, false);
                for (ClientSession s : owners) {
                    s.sendAll(lines);
                }
            }
        }

        if (subscribers.isEmpty() || (output.trades().isEmpty() && output.levelUpdates().isEmpty())) {
            return;
        }
        List<String> lines = new ArrayList<>(output.trades().size() + output.levelUpdates().size());
        for (Trade t : output.trades()) {
            lines.add(Protocol.trade(output.sequenceNumber(), t));
        }
        for (LevelUpdate l : output.levelUpdates()) {
            lines.add(Protocol.level(output.sequenceNumber(), l));
        }
        for (ClientSession s : subscribers) {
            s.sendAll(lines);
        }
    }

    // Stops accepting and drops every connection. Does not stop the exchange.
    @Override
    public void close() throws IOException {
        server.close();
        for (ClientSession s : sessions) {
            s.closeNow();
        }
    }
}
