package exchangeserver.gateway;

import exchangeserver.core.ExchangeException;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Rules;
import exchangeserver.engine.Response;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.US_ASCII;

/*

One TCP connection, with two threads:

reader  parses lines, applies the stateless checks (syntax, then OrderRequest), and hands valid requests to the
        exchange. Malformed or invalid requests are answered here and never reach the queue (D19, D30).
sender  writes the outbound queue to the socket.

When the reader stops (QUIT, end of stream, an overlong line), the session closes only after the exchange has
answered every request it already handed in (D63): a client that sends NEW then QUIT gets its ORDER, then BYE.

send() is called from the exchange's writer thread (replies, fills, market data) and must never block it, so
it only offers to a bounded queue. A client that cannot keep up is disconnected, never waited for (D54).

 */
final class ClientSession {

    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final String CLOSE = new String("<close>");     // sentinel, compared by identity

    final long id;
    private final Socket socket;
    private final TcpGateway gateway;
    private final BlockingQueue<String> outbound;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object subscriptionLock = new Object();
    private boolean subscriptionActive;     // guarded by subscriptionLock
    private long subscriptionNumber;        // guarded by subscriptionLock
    private final Thread reader;
    private final Thread sender;
    // Written by the reader thread at LOGIN, read by the writer thread when routing fills.
    private volatile String participant;

    ClientSession(long id, Socket socket, TcpGateway gateway, int outboundCapacity) {
        this.id = id;
        this.socket = socket;
        this.gateway = gateway;
        this.outbound = new ArrayBlockingQueue<>(outboundCapacity);
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
        String farewell = null;
        try {
            InputStream in = new BufferedInputStream(socket.getInputStream());
            String line;
            while ((line = readLine(in)) != null) {
                if (!handle(line)) {
                    farewell = "BYE";
                    break;
                }
            }
            answerFirst = true;                     // QUIT or end of stream
        } catch (ProtocolException e) {
            send(Protocol.error(e.getMessage()));
            answerFirst = true;
        } catch (IOException e) {
            // connection gone: nobody left to answer
        } finally {
            if (answerFirst) {
                closeAfterReplies(farewell);
            } else {
                close();
            }
        }
    }

    // Closes once every request this session handed in has been answered (D63). The exchange processes its queue
    // in order, so a read queued now completes after all of them, and their reply callbacks have already offered
    // their lines. A refused read (exchange stopping) completes at once: shutdown drops connections anyway.
    private void closeAfterReplies(String farewell) {
        gateway.service().snapshot(1, (snapshot, failure) -> {
            if (farewell != null) {
                send(farewell);
            }
            close();
        });
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
    private boolean handle(String line) {
        ClientMessage message;
        try {
            message = Protocol.parse(line);
        } catch (ProtocolException e) {
            send(Protocol.error(e.getMessage()));
            return true;
        }

        if (message instanceof ClientMessage.Login login) {
            login(login.participant());
            return true;
        }
        if (message instanceof ClientMessage.Ping) {
            send("PONG");
            return true;
        }
        if (message instanceof ClientMessage.Quit) {
            return false;                           // BYE follows the replies still owed (readLoop)
        }
        String p = participant;
        if (p == null) {
            send(Protocol.error("LOGIN first"));
            return true;
        }

        if (message instanceof ClientMessage.NewOrder n) {
            newOrder(p, n);
        } else if (message instanceof ClientMessage.Cancel c) {
            if (c.orderId() <= 0) {
                send(Protocol.error("order id must be positive"));
            } else {
                gateway.service().cancel(p, c.orderId(), (result, failure) ->
                        send(failure == null ? Protocol.cancel(result) : failureLine("orderId=" + c.orderId(), failure)));
            }
        } else if (message instanceof ClientMessage.Subscribe) {
            subscribe();
        } else if (message instanceof ClientMessage.Unsubscribe) {
            unsubscribe();
        }
        return true;
    }

    private void login(String requested) {
        if (participant != null) {
            send(Protocol.error("already logged in as " + participant));
            return;
        }
        if (!Rules.PARTICIPANT.matcher(requested).matches()) {
            send(Protocol.error("invalid participant " + requested));
            return;
        }
        participant = requested;
        gateway.loggedIn(this);
        // close() sets `closed` and then unregisters. If it ran before loggedIn(), its unregistering missed us:
        // undo it here. (Add-then-check against set-then-remove: at least one side sees the other.)
        if (closed.get()) {
            gateway.removed(this);
            return;
        }
        send(Protocol.welcome(requested));
    }

    private void newOrder(String p, ClientMessage.NewOrder n) {
        OrderRequest request;
        try {
            request = OrderRequest.of(p, n.clientOrderId(), n.side(), n.type(), n.quantity(), n.price());
        } catch (IllegalArgumentException invalid) {
            send(Protocol.error("clOrdId=" + n.clientOrderId() + " " + invalid.getMessage()));
            return;
        }
        // Runs on the writer thread (or here, if refused): FILLs then ORDER, before this command's market data.
        gateway.service().submit(request, (Response response, Throwable failure) -> {
            if (failure != null) {
                send(failureLine("clOrdId=" + n.clientOrderId(), failure));
                return;
            }
            List<String> lines = new ArrayList<>(response.trades().size() + 1);
            Protocol.takerFills(response, n.side(), n.quantity(), lines);
            lines.add(Protocol.order(response));
            sendAll(lines);
        });
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
    private void subscribe() {
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
        gateway.service().snapshot(Protocol.SNAPSHOT_DEPTH, (snapshot, failure) -> {
            synchronized (subscriptionLock) {
                if (mine != subscriptionNumber || !subscriptionActive) {
                    return;                         // withdrawn by an UNSUBSCRIBE that got here first
                }
                if (failure != null) {
                    subscriptionActive = false;
                    send(failureLine("SUBSCRIBE", failure));
                    return;
                }
                send("SUBSCRIBED");
                send(Protocol.book(snapshot));
                gateway.subscribe(this);
            }
            if (closed.get()) {
                gateway.unsubscribe(this);          // closed meanwhile: removed() may already have run
            }
        });
    }

    private void unsubscribe() {
        synchronized (subscriptionLock) {
            subscriptionActive = false;
            gateway.unsubscribe(this);
            send("UNSUBSCRIBED");
        }
    }

    private static String failureLine(String context, Throwable failure) {
        String reason = failure instanceof ExchangeException e ? e.getReason().name() : failure.getClass().getSimpleName();
        return Protocol.error(context + " " + reason);
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
