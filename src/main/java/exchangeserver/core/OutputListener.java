package exchangeserver.core;

import exchangeserver.engine.EngineOutput;

import java.util.List;

/*

Receives every command's output, in sequence order, on the writer thread: the engine output (market data: trades,
level updates) and the private events it produced (D73), numbered per participant, requester's reply first.
Called before the command's own future is completed, so a request callback runs after the request's lines went out
(D76). Must not block (it would stall the whole exchange) and should not throw (a throw is logged and ignored).
The gateway implements it.

Not called during replay: those outputs were already delivered in the live run (D41).

 */
@FunctionalInterface
public interface OutputListener {

    OutputListener NONE = (output, events) -> { };

    void onOutput(EngineOutput output, List<PrivateEvent> events);
}
