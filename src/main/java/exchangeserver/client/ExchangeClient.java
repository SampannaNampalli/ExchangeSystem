package exchangeserver.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.US_ASCII;

/*

A minimal client for the line protocol, used by ClientMain and by the gateway tests.

A background thread reads lines into a queue. await() returns the first line matching a predicate within a
timeout and keeps the lines it skipped, so a later await() can still find them (replies, fills and market data
interleave on one connection).

 */
public final class ExchangeClient implements AutoCloseable {

    private static final String EOF = "\u0000EOF";

    private final Socket socket;
    private final Writer out;
    private final BlockingQueue<String> incoming = new LinkedBlockingQueue<>();
    private final List<String> skipped = new ArrayList<>();

    public ExchangeClient(String host, int port) throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), US_ASCII));
        Thread reader = new Thread(this::readLoop, "client-reader");
        reader.setDaemon(true);
        reader.start();
    }

    public synchronized void send(String line) throws IOException {
        out.write(line);
        out.write('\n');
        out.flush();
    }

    // The first line (skipped ones first, oldest first) that matches, or an AssertionError-style exception.
    public String await(Predicate<String> match, Duration timeout) throws InterruptedException {
        for (Iterator<String> it = skipped.iterator(); it.hasNext(); ) {
            String line = it.next();
            if (match.test(line)) {
                it.remove();
                return line;
            }
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long left = deadline - System.nanoTime();
            String line = left <= 0 ? null : incoming.poll(left, TimeUnit.NANOSECONDS);
            if (line == null) {
                throw new IllegalStateException("No matching line within " + timeout + "; unmatched so far: " + skipped);
            }
            if (line == EOF) {
                incoming.add(EOF);
                throw new IllegalStateException("Connection closed; unmatched lines: " + skipped);
            }
            if (match.test(line)) {
                return line;
            }
            skipped.add(line);
        }
    }

    public String awaitPrefix(String prefix, Duration timeout) throws InterruptedException {
        return await(line -> line.startsWith(prefix), timeout);
    }

    // Every line, in arrival order (skipped ones first), up to and including the first that matches.
    public List<String> linesUntil(Predicate<String> match, Duration timeout) throws InterruptedException {
        List<String> lines = new ArrayList<>();
        String[] last = {null};
        await(line -> {
            lines.add(line);
            last[0] = line;
            return match.test(line);
        }, timeout);
        // await() moved the non-matching lines it read into `skipped`; they are reported here, so drop them.
        skipped.removeAll(lines);
        return lines;
    }

    // Blocks for the next raw line, or null at end of stream. For the interactive client.
    public String nextLine() throws InterruptedException {
        String line = incoming.take();
        if (line == EOF) {
            incoming.add(EOF);
            return null;
        }
        return line;
    }

    public boolean isClosedByServer(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String line = incoming.poll(10, TimeUnit.MILLISECONDS);
            if (line == EOF) {
                incoming.add(EOF);
                return true;
            }
            if (line != null) {
                skipped.add(line);
            }
        }
        return false;
    }

    private void readLoop() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), US_ASCII))) {
            String line;
            while ((line = in.readLine()) != null) {
                incoming.add(line);
            }
        } catch (IOException e) {
            // closed
        } finally {
            incoming.add(EOF);
        }
    }

    // Parses "key=value" out of a server line, e.g. field("ORDER orderId=7 ...", "orderId") -> "7".
    public static String field(String line, String key) {
        for (String token : line.split(" ")) {
            if (token.startsWith(key + "=")) {
                return token.substring(key.length() + 1);
            }
        }
        throw new IllegalArgumentException("No " + key + " in: " + line);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
