package exchangeserver.gateway;

import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;

import java.util.OptionalLong;

// One parsed line from a client. Syntax only: the session adds the participant and builds the OrderRequest.
sealed interface ClientMessage {

    // toString leaves the secret out: a parsed message may end up in a log.
    record Login(String participant, String secret, boolean cancelOnDisconnect) implements ClientMessage {
        @Override
        public String toString() {
            return "Login[" + participant + (cancelOnDisconnect ? ", COD" : "") + "]";
        }
    }

    record NewOrder(String clientOrderId, Side side, OrderType type, long quantity, OptionalLong price)
            implements ClientMessage {
    }

    record Cancel(long orderId) implements ClientMessage {
    }

    record Resend(long fromOseq) implements ClientMessage {
    }

    record Subscribe() implements ClientMessage {
    }

    record Unsubscribe() implements ClientMessage {
    }

    record Ping() implements ClientMessage {
    }

    record Quit() implements ClientMessage {
    }
}
