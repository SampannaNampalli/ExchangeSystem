package exchangeserver.core;

import exchangeserver.command.Command;

/*

Test seam: runs just before the engine processes a command, live and during replay. Tests use it to inject a
poison pill (throw) or to hold the writer thread (block). Production uses NONE.

 */
@FunctionalInterface
interface CommandInterceptor {

    CommandInterceptor NONE = command -> { };

    void beforeEngine(Command command);
}
