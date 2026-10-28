package exchangeserver;

import exchangeserver.core.Exchange;
import exchangeserver.core.ExchangeService;
import exchangeserver.core.ReplayException;
import exchangeserver.gateway.Credentials;
import exchangeserver.gateway.TcpGateway;
import exchangeserver.gateway.Tls;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/*

Starts the exchange: load credentials, bind the gateway, replay the journal, start the writer thread.

    java -jar exchange-server2.jar [--port 9000] [--bind 127.0.0.1] [--journal exchange.journal]
                                   [--users exchange.users] [--tls-keystore FILE]
                                   [--login-timeout 10] [--idle-timeout 300] [--max-in-flight 1000]
                                   [--rate 1000] [--burst 2000] [--check-interval 10000] [--resend-window 1000]
    java -jar exchange-server2.jar --users exchange.users --add-user <participant>    (asks for the secret)
    java -jar exchange-server2.jar --journal exchange.journal --quarantine <seq>
    java -jar exchange-server2.jar --journal exchange.journal --compact

--bind 0.0.0.0 exposes the gateway to the network: use it with --tls-keystore (password in the ES2_TLS_PASSWORD
environment variable), or secrets cross the network in clear text (D74).

Exit codes: 0 after a clean stop (Ctrl+C), 1 bad arguments, startup or tool failure, 2 the exchange halted
(restart it; the journal is the truth). Fail-stop: a halted exchange exits rather than limp along refusing
everything. The shutdown hook ends the process itself, so a stop by signal also exits 0.

 */
public final class Main {

    private Main() {
    }

    private static final class Options {
        int port = 9000;
        InetAddress bind = InetAddress.getLoopbackAddress();
        Path journal = Path.of("exchange.journal");
        Path users = Path.of("exchange.users");
        Path tlsKeystore;
        String addUser;
        Long quarantine;
        boolean compact;
        int loginTimeout = 10;
        int idleTimeout = 300;
        int maxInFlight = 1_000;
        int rate = 1_000;
        int burst = 2_000;
        int checkInterval = 10_000;
        int resendWindow = 1_000;
    }

    public static void main(String[] args) throws Exception {
        Options o = new Options();
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--port" -> o.port = Integer.parseInt(args[++i]);
                    case "--bind" -> o.bind = InetAddress.getByName(args[++i]);
                    case "--journal" -> o.journal = Path.of(args[++i]);
                    case "--users" -> o.users = Path.of(args[++i]);
                    case "--tls-keystore" -> o.tlsKeystore = Path.of(args[++i]);
                    case "--add-user" -> o.addUser = args[++i];
                    case "--quarantine" -> o.quarantine = Long.parseLong(args[++i]);
                    case "--compact" -> o.compact = true;
                    case "--login-timeout" -> o.loginTimeout = Integer.parseInt(args[++i]);
                    case "--idle-timeout" -> o.idleTimeout = Integer.parseInt(args[++i]);
                    case "--max-in-flight" -> o.maxInFlight = Integer.parseInt(args[++i]);
                    case "--rate" -> o.rate = Integer.parseInt(args[++i]);
                    case "--burst" -> o.burst = Integer.parseInt(args[++i]);
                    case "--check-interval" -> o.checkInterval = Integer.parseInt(args[++i]);
                    case "--resend-window" -> o.resendWindow = Integer.parseInt(args[++i]);
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
        } catch (RuntimeException | IOException e) {
            fail("Usage: see the comment at the top of Main.java (" + e.getMessage() + ")");
            return;
        }

        if (o.addUser != null) {
            tool(() -> {
                Credentials.addUser(o.users, o.addUser, readSecret(o.addUser), Credentials.DEFAULT_ITERATIONS);
                System.out.println("Saved " + o.addUser + " in " + o.users.toAbsolutePath());
            }, "Adding the user");
            return;
        }
        if (o.quarantine != null) {
            tool(() -> {
                Exchange.quarantine(o.journal, o.quarantine, Clock.systemUTC());
                System.out.println("Quarantined command " + o.quarantine + " in " + o.journal.toAbsolutePath());
            }, "Quarantine");
            return;
        }
        if (o.compact) {
            tool(() -> {
                Exchange.Compaction c = Exchange.compact(o.journal);
                System.out.println(c == null ? "Nothing to compact: the journal has no records since its snapshot"
                        : "Compacted through sequence " + c.lastSequence() + ": " + c.restingOrders()
                        + " resting orders in " + c.snapshot() + "; old journal archived as " + c.archive());
            }, "Compaction");
            return;
        }
        serve(o);
    }

    private static void serve(Options o) throws Exception {
        TcpGateway gateway;
        ExchangeService service;
        try {
            Credentials credentials = Credentials.load(o.users);
            SSLContext tls = o.tlsKeystore == null ? null
                    : Tls.serverContext(o.tlsKeystore, Tls.passwordFromEnvironment("ES2_TLS_PASSWORD"));
            if (tls == null && !o.bind.isLoopbackAddress()) {
                System.err.println("Warning: listening on " + o.bind.getHostAddress() + " without TLS: secrets cross the network in clear text.");
            }
            gateway = new TcpGateway(TcpGateway.Settings.defaults(o.port, credentials)
                    .withBindAddress(o.bind)
                    .withTls(tls)
                    .withTimeouts(Duration.ofSeconds(o.loginTimeout), Duration.ofSeconds(o.idleTimeout))
                    .withFlowControl(o.maxInFlight, o.rate, o.burst));
        } catch (IOException | RuntimeException e) {
            fail("Startup failed: " + e.getMessage());
            return;
        }
        gateway.startAccepting();                   // clients get "starting" during replay instead of silence (L6)
        try {
            service = ExchangeService.start(o.journal, gateway, ExchangeService.Settings.defaults()
                    .withExchangeOptions(new Exchange.Options(o.checkInterval, o.resendWindow)));
        } catch (IOException | RuntimeException e) {
            gateway.close();
            System.err.println("Startup failed: " + e.getMessage());
            System.err.println(hint(e));
            System.exit(1);
            return;
        }
        gateway.attach(service);
        Exchange.Recovery r = service.recovery();
        System.out.println("ExchangeServer2 listening on " + o.bind.getHostAddress() + ":" + gateway.port()
                + (o.tlsKeystore == null ? "" : " (TLS)") + ", journal " + o.journal.toAbsolutePath());
        System.out.println("Recovered through sequence " + r.lastSequence() + (r.baseSequence() > 0 ? " from snapshot " + r.baseSequence() : "")
                + ": replayed " + r.replayed() + " commands, cut " + r.truncatedBytes() + " torn bytes"
                + (r.quarantined().isEmpty() ? "" : ", quarantined " + r.quarantined())
                + (r.adoptedRulesFrom() == 0 ? "" : ", journal moved from rules " + r.adoptedRulesFrom()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            int status = 0;
            try {
                gateway.close();
                if (!service.stop(Duration.ofSeconds(10))) {
                    System.err.println("Writer thread did not stop within 10 s");
                    status = 1;
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Shutdown: " + e);
                status = 1;
            }
            if (service.state() == ExchangeService.State.HALTED) {
                status = 2;
            }
            System.out.flush();
            System.err.flush();
            Runtime.getRuntime().halt(status);     // the documented code, whatever started the shutdown
        }, "shutdown"));

        service.awaitTermination();
        if (service.state() == ExchangeService.State.HALTED) {
            System.err.println("Exchange halted: " + service.haltCause());
            System.exit(2);
        }
    }

    // L2: advice that fits the failure.
    static String hint(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ReplayException r) {
                return r.kind() == ReplayException.Kind.ENGINE_FAILURE
                        ? "Replay failed on command " + r.sequenceNumber() + " (a poison pill?). Inspect it, then run --quarantine "
                        + r.sequenceNumber() + " and start again."
                        : "This build replays the journal differently from the build that wrote it. Start the old build, run "
                        + "--compact, then start this build.";
            }
        }
        return "The journal was not changed.";
    }

    private interface Tool {
        void run() throws Exception;
    }

    private static void tool(Tool tool, String what) {
        try {
            tool.run();
        } catch (Exception e) {
            fail(what + " failed: " + e.getMessage());
        }
    }

    private static String readSecret(String participant) throws IOException {
        Console console = System.console();
        if (console != null) {
            char[] first = console.readPassword("Secret for %s: ", participant);
            char[] again = console.readPassword("Again: ");
            if (first == null || !Arrays.equals(first, again)) {
                throw new IllegalArgumentException("the secrets differ");
            }
            return new String(first);
        }
        // No terminal (a script): one line from standard input. Windows PowerShell pipes text with a UTF-8
        // byte-order mark in front; it is not part of the secret.
        String line = new BufferedReader(new InputStreamReader(System.in, UTF_8)).readLine();
        if (line == null) {
            throw new IllegalArgumentException("no secret on standard input");
        }
        if (!line.isEmpty() && line.charAt(0) == (char) 0xFEFF) {
            line = line.substring(1);
        }
        return line.strip();
    }

    private static void fail(String message) {
        System.err.println(message);
        System.exit(1);
    }
}
