package exchangeserver.gateway;

import exchangeserver.book.LevelUpdate;
import exchangeserver.core.ExchangeService;
import exchangeserver.core.OutputListener;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.Execution;
import exchangeserver.engine.Trade;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.US_ASCII;

/*

The TCP gateway (roadmap step 4B). Plain TCP, a line protocol (Protocol), thread-per-connection.

Threads: one acceptor; per connection a reader and a sender (ClientSession). The exchange's writer thread calls
onOutput() and the reply callbacks; both only offer lines to session queues, so no client can slow the exchange.

Routing:
    reply to a request           the session that sent it
    passive fill (Execution)     every session logged in as the resting order's participant
    market data (TRADE, LEVEL)   every subscribed session

A fill for a participant with no open session is not stored; there is no resend yet (Spec: open threads).

Usage: create (binds the port), pass to ExchangeService.start as its OutputListener, then start(service).

 */
public final class TcpGateway implements OutputListener, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(TcpGateway.class.getName());
    private static final long ACCEPT_BACKOFF_MILLIS = 100;

    private final ServerSocket server;
    private final int maxSessions;
    private final int outboundCapacity;
    private final AtomicLong sessionIds = new AtomicLong();
    private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();
    private final Set<ClientSession> subscribers = ConcurrentHashMap.newKeySet();
    private final Map<String, Set<ClientSession>> sessionsByParticipant = new ConcurrentHashMap<>();
    private volatile ExchangeService service;
    private Thread acceptor;

    public TcpGateway(int port) throws IOException {
        this(port, 256, 10_000);
    }

    public TcpGateway(int port, int maxSessions, int outboundCapacity) throws IOException {
        this.maxSessions = maxSessions;
        this.outboundCapacity = outboundCapacity;
        this.server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(port));
    }

    public int port() {
        return server.getLocalPort();
    }

    public synchronized void start(ExchangeService exchangeService) {
        if (acceptor != null) {
            throw new IllegalStateException("Already started");
        }
        this.service = Objects.requireNonNull(exchangeService, "exchangeService");
        acceptor = new Thread(this::acceptLoop, "gateway-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    ExchangeService service() {
        return service;
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            Socket socket;
            try {
                socket = server.accept();
            } catch (IOException e) {
                // Only a closed server socket ends the loop. Anything else (e.g. "Too many open files", also a
                // SocketException) is passing: log, back off, keep accepting.
                if (server.isClosed()) {
                    return;
                }
                LOG.log(Level.WARNING, "accept failed; retrying", e);
                try {
                    Thread.sleep(ACCEPT_BACKOFF_MILLIS);
                } catch (InterruptedException interrupted) {
                    return;
                }
                continue;
            }
            try {
                socket.setTcpNoDelay(true);
                if (sessions.size() >= maxSessions) {
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
            ClientSession session = new ClientSession(sessionIds.incrementAndGet(), socket, this, outboundCapacity);
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
        if (p != null) {
            // Drop the participant's entry with its last session, so ever-new names cannot grow the map.
            sessionsByParticipant.computeIfPresent(p, (k, mine) -> {
                mine.remove(session);
                return mine.isEmpty() ? null : mine;
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

    void log(String message) {
        LOG.log(Level.INFO, message);
    }

    // ---------- Writer thread ----------

    @Override
    public void onOutput(EngineOutput output) {
        for (Execution e : output.passiveFills()) {
            Set<ClientSession> owners = sessionsByParticipant.get(e.participant());
            if (owners != null) {
                String line = Protocol.makerFill(e);
                for (ClientSession s : owners) {
                    s.send(line);
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
