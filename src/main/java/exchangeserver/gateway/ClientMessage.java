package exchangeserver.gateway;

import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;

import java.util.OptionalLong;

// One parsed line from a client. Syntax only: the session adds the participant and builds the OrderRequest.
sealed interface ClientMessage {

    record Login(String participant) implements ClientMessage {
    }

    record NewOrder(String clientOrderId, Side side, OrderType type, long quantity, OptionalLong price)
            implements ClientMessage {
    }

    record Cancel(long orderId) implements ClientMessage {
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
