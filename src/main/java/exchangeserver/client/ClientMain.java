package exchangeserver.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;

/*

Interactive console client: type protocol lines (see gateway.Protocol), see every server line as it arrives.

    java -cp exchange-server2.jar exchangeserver.client.ClientMain [host] [port]

 */
public final class ClientMain {

    private ClientMain() {
    }

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;

        try (ExchangeClient client = new ExchangeClient(host, port)) {
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

            System.out.println("Connected to " + host + ":" + port + ". Try: LOGIN alice / NEW c1 BUY LIMIT 10 100 / SUBSCRIBE / QUIT");
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
