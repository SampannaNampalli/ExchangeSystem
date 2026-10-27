package exchangeserver.core;

import exchangeserver.engine.EngineOutput;

/*

Receives every command's output, in sequence order, on the writer thread, after the command's own future has
been completed. Must not block (it would stall the whole exchange) and should not throw (a throw is logged and
ignored). The gateway uses it for passive fills and market data.

Not called during replay: those outputs were already delivered in the live run (D41).

 */
@FunctionalInterface
public interface OutputListener {

    OutputListener NONE = output -> { };

    void onOutput(EngineOutput output);
}
