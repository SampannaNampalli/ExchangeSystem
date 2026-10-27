package exchangeserver.core;

import java.util.List;

// The answer to a RESEND (D73): the retained private events from the requested number on, the first number still
// retained (a request below it has a gap the exchange cannot fill), and the participant's latest number.
public record ResendResult(List<PrivateEvent> events, long firstAvailable, long lastOseq) {

    public ResendResult {
        events = List.copyOf(events);
    }
}
