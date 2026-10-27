package exchangeserver;

import exchangeserver.core.Exchange;
import exchangeserver.core.ExchangeService;
import exchangeserver.gateway.TcpGateway;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;

/*

Starts the exchange: replay the journal, start the writer thread, open the TCP gateway.

    java -jar exchange-server2.jar [--port 9000] [--journal exchange.journal]
    java -jar exchange-server2.jar --journal exchange.journal --quarantine <seq>

Exit codes: 0 after a clean stop (Ctrl+C), 1 bad arguments or startup failure, 2 the exchange halted (restart
it; the journal is the truth). Fail-stop: a halted exchange exits rather than limp along refusing everything.

 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        int port = 9000;
        Path journal = Path.of("exchange.journal");
        Long quarantine = null;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--port" -> port = Integer.parseInt(args[++i]);
                    case "--journal" -> journal = Path.of(args[++i]);
                    case "--quarantine" -> quarantine = Long.parseLong(args[++i]);
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
        } catch (RuntimeException e) {
            System.err.println("Usage: [--port 9000] [--journal exchange.journal] [--quarantine <seq>]  (" + e.getMessage() + ")");
            System.exit(1);
            return;
        }

        if (quarantine != null) {
            Exchange.quarantine(journal, quarantine, Clock.systemUTC());
            System.out.println("Quarantined command " + quarantine + " in " + journal.toAbsolutePath());
            return;
        }

        TcpGateway gateway = new TcpGateway(port);
        ExchangeService service;
        try {
            service = ExchangeService.start(journal, gateway);
        } catch (IOException | RuntimeException e) {
            gateway.close();
            System.err.println("Startup failed: " + e);
            System.err.println("If replay failed on one command (a poison pill), inspect it, then restart with --quarantine <seq>.");
            System.exit(1);
            return;
        }
        gateway.start(service);
        System.out.println("ExchangeServer2 listening on port " + gateway.port() + ", journal " + journal.toAbsolutePath());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                gateway.close();
                if (!service.stop(Duration.ofSeconds(10))) {
                    System.err.println("Writer thread did not stop within 10 s");
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Shutdown: " + e);
            }
        }, "shutdown"));

        service.awaitTermination();
        if (service.state() == ExchangeService.State.HALTED) {
            System.err.println("Exchange halted: " + service.haltCause());
            gateway.close();
            System.exit(2);
        }
    }
}
