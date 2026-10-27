package exchangeserver.command;

import java.time.Instant;

/*

A stamped, journalled input: the source of truth (D12, D31). Immutable, so the object that is logged and the
object that is processed cannot differ. The Java type is the kind tag; the journal writes it as a word.

 */
public sealed interface Command permits NewOrderCommand, CancelOrderCommand, SkipCommand {

    long sequenceNumber();

    Instant timestamp();
}
