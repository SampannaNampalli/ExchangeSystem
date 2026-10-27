package exchangeserver.client;

import exchangeserver.gateway.Tls;

import javax.net.SocketFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;

/*

Interactive console client: type protocol lines (see gateway.Protocol), see every server line as it arrives.

    java -cp exchange-server2.jar exchangeserver.client.ClientMain [host] [port] [--tls [--truststore FILE]]

--tls connects with TLS (D74). Without --truststore the JDK's trusted certificates apply; with it, the truststore's
password comes from the ES2_TRUSTSTORE_PASSWORD environment variable.

 */
public final class ClientMain {

    private ClientMain() {
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9000;
        boolean tls = false;
        Path truststore = null;
        int positional = 0;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--tls" -> tls = true;
                case "--truststore" -> truststore = Path.of(args[++i]);
                default -> {
                    if (positional++ == 0) {
                        host = args[i];
                    } else {
                        port = Integer.parseInt(args[i]);
                    }
                }
            }
        }
        SocketFactory factory = !tls ? SocketFactory.getDefault()
                : Tls.clientContext(truststore, truststore == null ? null : Tls.passwordFromEnvironment("ES2_TRUSTSTORE_PASSWORD"))
                        .getSocketFactory();

        try (ExchangeClient client = new ExchangeClient(host, port, factory)) {
            Thread printer = new Thread(() -> {
                try {
                    String line;
                    while ((line = client.nextLine()) != null) {
                        System.out.println("< " + line);
                    }
                    System.out.println("(connection closed)");
                } catch (InterruptedException ignored) {
                    // exiting
                }
            }, "printer");
            printer.setDaemon(true);
            printer.start();

            System.out.println("Connected to " + host + ":" + port + (tls ? " (TLS)" : "")
                    + ". Try: LOGIN alice <secret> / NEW c1 BUY LIMIT 10 100 / SUBSCRIBE / QUIT");
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, UTF_8));
            String line;
            while ((line = stdin.readLine()) != null) {
                client.send(line.strip());
                if (line.strip().equals("QUIT")) {
                    printer.join(2000);
                    break;
                }
            }
        }
    }
}
