package exchangeserver.gateway;

// A malformed line. Answered with ERROR at the gateway; never reaches the exchange.
final class ProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    ProtocolException(String message) {
        super(message);
    }
}
