package exchangeserver.gateway;

import exchangeserver.core.ExchangeException;
import exchangeserver.core.ExchangeService;
import exchangeserver.core.PrivateEvent;
import exchangeserver.core.ResendResult;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static java.nio.charset.StandardCharsets.US_ASCII;

/*

One TCP connection, with two threads:

reader  parses lines, applies the stateless checks (syntax, then OrderRequest), the session's limits (D75) and
        hands valid requests to the exchange. Malformed, invalid or over-limit requests are answered here and never
        reach the queue (D19, D30).
sender  writes the outbound queue to the socket.

Login (D74): LOGIN <participant> <secret> [COD]. The secret is checked on the reader thread (one attempt per
connection); then an Attach read joins the participant's private stream on the writer thread, so the WELCOME's
oseq and the first event the session receives leave no gap (D73).

Limits (D75): a login timeout and an idle timeout (socket read timeouts); at most maxInFlight requests handed in
and not yet answered; a token-bucket rate limit on requests. A refusal is an ERROR line, never queued.

Closing (D76): when the reader stops (QUIT, end of stream, overlong line, timeout), the session closes only once
every request it handed in has completed: a request's callback runs after its lines were offered, so the last
callback to finish sends BYE (for QUIT) and closes. This holds during shutdown too, since every accepted request
completes (processed, or refused). A broken connection closes at once: nobody is left to answer.

send() is called from the exchange's writer thread (replies, fills, market data) and must never block it, so
it only offers to a bounded queue. A client that cannot keep up is disconnected, never waited for (D54).

 */
final class ClientSession {

    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final String CLOSE = new String("<close>");     // sentinel, compared by identity

    final long id;
    private final Socket socket;
    private final TcpGateway gateway;
    private final TcpGateway.Settings settings;
    private final BlockingQueue<String> outbound;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object subscriptionLock = new Object();
    private boolean subscriptionActive;     // guarded by subscriptionLock
    private long subscriptionNumber;        // guarded by subscriptionLock
    private final Thread reader;
    private final Thread sender;
    // Written by the reader thread at LOGIN, read by the writer thread when routing and by removed().
    private volatile String participant;
    private volatile boolean cancelOnDisconnect;

    // D76: requests handed in and not yet completed; the reader's end; the farewell line; closed once.
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean readerDone;
    private volatile String farewell;
    private final AtomicBoolean finished = new AtomicBoolean();

    // Rate limit (D75), reader thread only.
    private double tokens;
    private long lastRefillNanos = System.nanoTime();
    private boolean loginAttempted;

    ClientSession(long id, Socket socket, TcpGateway gateway) {
        this.id = id;
        this.socket = socket;
        this.gateway = gateway;
        this.settings = gateway.settings();
        this.outbound = new ArrayBlockingQueue<>(settings.outboundCapacity());
        this.tokens = settings.burst();
        this.reader = new Thread(this::readLoop, "session-" + id + "-reader");
        this.sender = new Thread(this::sendLoop, "session-" + id + "-sender");
        reader.setDaemon(true);
        sender.setDaemon(true);
    }

    void start() {
        sender.start();
        reader.start();
    }

    String participant() {
        return participant;
    }

    boolean cancelOnDisconnect() {
        return cancelOnDisconnect;
    }

    // For tests: the reader stopped, and the session waits for requests still in flight (D76).
    boolean draining() {
        return readerDone && !finished.get();
    }

    // Any thread; never blocks.
    void send(String line) {
        if (closed.get()) {
            return;
        }
        if (!outbound.offer(line)) {
            gateway.log("session " + id + " is too slow; disconnecting");
            closeNow();
        }
    }

    void sendAll(List<String> lines) {
        for (String line : lines) {
            send(line);
        }
    }

    // Graceful: the sender writes what is queued (e.g. BYE), then closes the socket.
    void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        gateway.removed(this);
        if (!outbound.offer(CLOSE)) {
            closeSocket();
        }
    }

    // Immediate: pending output is dropped.
    void closeNow() {
        close();
        closeSocket();
        sender.interrupt();
    }

    // ---------- Reader thread ----------

    private void readLoop() {
        // Not try-with-resources: closing a socket's input stream closes the whole socket, and the sender must
        // still flush what is queued (replies, BYE, a final ERROR). The sender closes the socket.
        boolean answerFirst = false;
        try {
            socket.setSoTimeout((int) settings.loginTimeout().toMillis());
            InputStream in = new BufferedInputStream(socket.getInputStream());
            String line;
            while ((line = readLine(in)) != null) {
                if (!handle(line)) {
                    break;
                }
            }
            answerFirst = true;                     // QUIT, a failed login, or end of stream
        } catch (SocketTimeoutException e) {
            send(Protocol.error(participant == null ? "login timeout; disconnecting" : "idle timeout; disconnecting"));
            answerFirst = true;
        } catch (ProtocolException e) {
            send(Protocol.error(e.getMessage()));
            answerFirst = true;
        } catch (IOException e) {
            // connection gone: nobody left to answer
        } finally {
            if (answerFirst) {
                readerDone = true;
                finishIfIdle();
            } else {
                close();
            }
        }
    }

    // A line without its terminator, or null at end of stream. Bounded: a client cannot make us buffer more than
    // MAX_LINE_LENGTH + 1 bytes (the +1 is room for the '\r' of "\r\n", which does not count toward the limit).
    static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                byte[] bytes = line.toByteArray();
                int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r' ? bytes.length - 1 : bytes.length;
                if (length > Protocol.MAX_LINE_LENGTH) {
                    throw tooLong();
                }
                return new String(bytes, 0, length, US_ASCII);
            }
            if (line.size() > Protocol.MAX_LINE_LENGTH) {
                throw tooLong();
            }
            line.write(b);
        }
        return null;            // a partial last line is dropped: it was never terminated
    }

    private static ProtocolException tooLong() {
        return new ProtocolException("line longer than " + Protocol.MAX_LINE_LENGTH + " bytes; disconnecting");
    }

    // Returns false to stop reading.
    private boolean handle(String line) throws IOException {
        ClientMessage message;
        try {
            message = Protocol.parse(line);
        } catch (ProtocolException e) {
            send(Protocol.error(e.getMessage()));
            return true;
        }

        if (message instanceof ClientMessage.Ping) {
            send("PONG");
            return true;
        }
        if (message instanceof ClientMessage.Quit) {
            farewell = "BYE";                       // sent after the replies still owed (D76)
            return false;
        }
        ExchangeService service = gateway.service();
        if (service == null) {
            send(Protocol.error("exchange is starting; retry shortly"));        // L6
            return false;
        }
        if (message instanceof ClientMessage.Login login) {
            return login(service, login);
        }
        String p = participant;
        if (p == null) {
            send(Protocol.error("LOGIN first"));
            return true;
        }
        if (message instanceof ClientMessage.Unsubscribe) {
            unsubscribe();
            return true;
        }

        String context = context(message);
        String refusal = admit();
        if (refusal != null) {
            send(Protocol.error(context + " " + refusal));
            return true;
        }
        if (message instanceof ClientMessage.NewOrder n) {
            newOrder(service, p, n, context);
        } else if (message instanceof ClientMessage.Cancel c) {
            if (c.orderId() <= 0) {
                send(Protocol.error("order id must be positive"));
            } else {
                service.cancel(p, c.orderId(), tracked((result, failure) -> replyFailure(context, failure)));
            }
        } else if (message instanceof ClientMessage.Resend r) {
            resend(service, p, r.fromOseq());
        } else if (message instanceof ClientMessage.Subscribe) {
            subscribe(service);
        }
        return true;
    }

    private static String context(ClientMessage message) {
        if (message instanceof ClientMessage.NewOrder n) {
            return "clOrdId=" + n.clientOrderId();
        }
        if (message instanceof ClientMessage.Cancel c) {
            return "orderId=" + c.orderId();
        }
        return message instanceof ClientMessage.Resend ? "RESEND" : "SUBSCRIBE";
    }

    // D75: null if the request may go to the exchange, else why not.
    private String admit() {
        if (inFlight.get() >= settings.maxInFlight()) {
            return "SESSION_BUSY";
        }
        long now = System.nanoTime();
        tokens = Math.min(settings.burst(), tokens + (now - lastRefillNanos) * settings.ratePerSecond() / 1e9);
        lastRefillNanos = now;
        if (tokens < 1) {
            return "RATE_LIMITED";
        }
        tokens -= 1;
        return null;
    }

    private boolean login(ExchangeService service, ClientMessage.Login login) throws IOException {
        if (participant != null) {
            send(Protocol.error("already logged in as " + participant));
            return true;
        }
        if (loginAttempted) {
            send(Protocol.error("one login attempt per connection"));
            return false;
        }
        loginAttempted = true;
        String requested = login.participant();
        if (!Rules.PARTICIPANT.matcher(requested).matches() || !gateway.authenticate(requested, login.secret())) {
            send(Protocol.error("login failed"));       // the same answer whatever was wrong (D74)
            return false;
        }
        socket.setSoTimeout((int) settings.idleTimeout().toMillis());
        cancelOnDisconnect = login.cancelOnDisconnect();
        participant = requested;
        // On the writer thread, before any later output: register for routing, then report the latest oseq (D73).
        service.attach(requested, tracked((lastOseq, failure) -> {
            if (failure != null) {
                replyFailure("LOGIN", failure);
                return;
            }
            gateway.loggedIn(this);
            // close() sets `closed` and then unregisters. If it ran before loggedIn(), its unregistering missed us:
            // undo it here. (Add-then-check against set-then-remove: at least one side sees the other.)
            if (closed.get()) {
                gateway.removed(this);
                return;
            }
            send(Protocol.welcome(requested, lastOseq));
        }));
        return true;
    }

    private void newOrder(ExchangeService service, String p, ClientMessage.NewOrder n, String context) {
        OrderRequest request;
        try {
            request = OrderRequest.of(p, n.clientOrderId(), n.side(), n.type(), n.quantity(), n.price());
        } catch (IllegalArgumentException invalid) {
            send(Protocol.error(context + " " + invalid.getMessage()));
            return;
        }
        // The FILL and ORDER lines come through the participant's private stream (D73); this callback only
        // reports a refusal.
        service.submit(request, tracked((response, failure) -> replyFailure(context, failure)));
    }

    private void resend(ExchangeService service, String p, long from) {
        if (from <= 0) {
            send(Protocol.error("oseq must be positive"));
            return;
        }
        // On the writer thread: the events go out before any later live event.
        service.resend(p, from, tracked((ResendResult result, Throwable failure) -> {
            if (failure != null) {
                replyFailure("RESEND", failure);
                return;
            }
            if (from < result.firstAvailable() && from <= result.lastOseq()) {
                send(Protocol.error("RESEND_GAP first=" + result.firstAvailable()));
            }
            for (PrivateEvent e : result.events()) {
                sendAll(Protocol.lines(e, true));
            }
            send("RESENT last=" + result.lastOseq());
        }));
    }

    /*
    Subscription state: SUBSCRIBE runs on the reader thread, its snapshot callback on the writer thread, UNSUBSCRIBE
    on the reader thread again. All three take subscriptionLock, so each happens entirely before or after the
    others, and the client's last word always matches its real state:
      - UNSUBSCRIBE before the snapshot arrives: the callback sees the claim withdrawn and sends nothing, so the
        client hears only UNSUBSCRIBED (no stale SUBSCRIBED/BOOK after it).
      - SUBSCRIBE, UNSUBSCRIBE, SUBSCRIBE: each SUBSCRIBE gets a number; only the callback holding the current
        number acts, so one BOOK goes out, not two.
    The lock is held only while offering lines to the outbound queue (never blocks), so the writer is not delayed.
    */
    private void subscribe(ExchangeService service) {
        long mine;
        synchronized (subscriptionLock) {
            if (subscriptionActive) {
                send(Protocol.error("already subscribed"));
                return;
            }
            subscriptionActive = true;
            mine = ++subscriptionNumber;
        }
        // The snapshot is taken on the writer thread and the session joins the subscribers in the same step,
        // so the first update it gets is the first command after the snapshot: no gap, no duplicate.
        service.snapshot(Protocol.SNAPSHOT_DEPTH, tracked((snapshot, failure) -> {
            synchronized (subscriptionLock) {
                if (mine != subscriptionNumber || !subscriptionActive) {
                    return;                         // withdrawn by an UNSUBSCRIBE that got here first
                }
                if (failure != null) {
                    subscriptionActive = false;
                    replyFailure("SUBSCRIBE", failure);
                    return;
                }
                send("SUBSCRIBED");
                send(Protocol.book(snapshot));
                gateway.subscribe(this);
            }
            if (closed.get()) {
                gateway.unsubscribe(this);          // closed meanwhile: removed() may already have run
            }
        }));
    }

    private void unsubscribe() {
        synchronized (subscriptionLock) {
            subscriptionActive = false;
            gateway.unsubscribe(this);
            send("UNSUBSCRIBED");
        }
    }

    private void replyFailure(String context, Throwable failure) {
        if (failure != null) {
            String reason = failure instanceof ExchangeException e ? e.getReason().name() : failure.getClass().getSimpleName();
            send(Protocol.error(context + " " + reason));
        }
    }

    // D76: counts the request in flight until its callback has run (after the request's own lines went out).
    private <T> BiConsumer<T, Throwable> tracked(BiConsumer<T, Throwable> callback) {
        inFlight.incrementAndGet();
        return (value, failure) -> {
            try {
                callback.accept(value, failure);
            } finally {
                inFlight.decrementAndGet();
                finishIfIdle();
            }
        };
    }

    // Closes after the reader stopped and nothing is in flight; whichever of the two happens last does it.
    private void finishIfIdle() {
        if (readerDone && inFlight.get() == 0 && finished.compareAndSet(false, true)) {
            String last = farewell;
            if (last != null) {
                send(last);
            }
            close();
        }
    }

    // ---------- Sender thread ----------

    private void sendLoop() {
        try (Writer out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), US_ASCII))) {
            while (true) {
                String line = outbound.take();
                if (line == CLOSE) {
                    break;
                }
                out.write(line);
                out.write('\n');
                if (outbound.isEmpty()) {
                    out.flush();        // flush once per burst, not per line
                }
            }
            out.flush();
        } catch (IOException | InterruptedException e) {
            // connection gone or forced close
        } finally {
            closeSocket();
            close();
        }
    }

    private void closeSocket() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing left to do
        }
    }
}
