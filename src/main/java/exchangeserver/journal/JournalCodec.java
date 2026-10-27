package exchangeserver.journal;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.OptionalLong;

/*

The journal's text format (D36), one record per line, ','-separated, every record ending in '\n':

    HEADER,<formatVersion>,<rulesVersion>                                                   (first record only)
    NEW,<seq>,<timestamp>,<orderId>,<participant>,<clientOrderId>,<side>,<type>,<quantity>,<price>
    CANCEL,<seq>,<timestamp>,<participant>,<targetOrderId>
    SKIP,<seq>,<timestamp>,<skippedSeq>

A MARKET order's price field is "-" (D26). Enums by name(); timestamps by Instant.toString() (ISO-8601, exact
to the nanosecond). Participant and client order id cannot contain ',' (Rules).

Decoding rebuilds requests through OrderRequest's factories, so the journal applies the same stateless rules as
the gateway (D19). Anything that does not parse is corrupt journal data: IllegalStateException (D27).

 */
final class JournalCodec {

    static final int FORMAT_VERSION = 2;
    static final String HEADER = "HEADER";
    static final String NEW = "NEW";
    static final String CANCEL = "CANCEL";
    static final String SKIP = "SKIP";
    static final String NO_PRICE = "-";
    static final char SEPARATOR = ',';
    static final char END_OF_RECORD = '\n';

    private JournalCodec() {
    }

    static String encodeHeader(int rulesVersion) {
        return HEADER + SEPARATOR + FORMAT_VERSION + SEPARATOR + rulesVersion + END_OF_RECORD;
    }

    // Returns the rules version; throws if this is not a header this code can read.
    static int decodeHeader(String record) {
        String[] f = record.split(String.valueOf(SEPARATOR), -1);
        try {
            if (!f[0].equals(HEADER)) {
                throw new IllegalArgumentException("first record is not a header");
            }
            requireFieldCount(f, 3);
            int format = Integer.parseInt(f[1]);
            if (format != FORMAT_VERSION) {
                throw new IllegalArgumentException("format version " + format + ", this code reads " + FORMAT_VERSION);
            }
            return Integer.parseInt(f[2]);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Bad journal header \"" + record + "\": " + e.getMessage(), e);
        }
    }

    static String encode(Command command) {
        Objects.requireNonNull(command, "command");
        StringBuilder sb = new StringBuilder(96);
        if (command instanceof NewOrderCommand n) {
            OrderRequest r = n.request();
            sb.append(NEW)
                    .append(SEPARATOR).append(n.sequenceNumber())
                    .append(SEPARATOR).append(n.timestamp())
                    .append(SEPARATOR).append(n.orderId())
                    .append(SEPARATOR).append(r.getParticipant())
                    .append(SEPARATOR).append(r.getClientOrderId())
                    .append(SEPARATOR).append(r.getSide().name())
                    .append(SEPARATOR).append(r.getOrderType().name())
                    .append(SEPARATOR).append(r.getQuantity())
                    .append(SEPARATOR).append(r.getPrice().isPresent() ? Long.toString(r.getPrice().getAsLong()) : NO_PRICE);
        } else if (command instanceof CancelOrderCommand c) {
            sb.append(CANCEL)
                    .append(SEPARATOR).append(c.sequenceNumber())
                    .append(SEPARATOR).append(c.timestamp())
                    .append(SEPARATOR).append(c.participant())
                    .append(SEPARATOR).append(c.orderId());
        } else if (command instanceof SkipCommand s) {
            sb.append(SKIP)
                    .append(SEPARATOR).append(s.sequenceNumber())
                    .append(SEPARATOR).append(s.timestamp())
                    .append(SEPARATOR).append(s.skippedSequenceNumber());
        } else {
            throw new IllegalStateException("Unknown command type " + command.getClass());
        }
        return sb.append(END_OF_RECORD).toString();
    }

    // record: one record without its '\n'. recordNumber: 1-based, header included, for the error message.
    static Command decode(String record, long recordNumber) {
        String[] f = record.split(String.valueOf(SEPARATOR), -1);     // -1 keeps trailing empty fields
        try {
            return switch (f[0]) {
                case NEW -> {
                    requireFieldCount(f, 10);
                    OrderType type = OrderType.valueOf(f[7]);
                    OptionalLong price = f[9].equals(NO_PRICE) ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(f[9]));
                    OrderRequest request = OrderRequest.of(f[4], f[5], Side.valueOf(f[6]), type, Long.parseLong(f[8]), price);
                    yield new NewOrderCommand(Long.parseLong(f[1]), Instant.parse(f[2]), Long.parseLong(f[3]), request);
                }
                case CANCEL -> {
                    requireFieldCount(f, 5);
                    yield new CancelOrderCommand(Long.parseLong(f[1]), Instant.parse(f[2]), f[3], Long.parseLong(f[4]));
                }
                case SKIP -> {
                    requireFieldCount(f, 4);
                    yield new SkipCommand(Long.parseLong(f[1]), Instant.parse(f[2]), Long.parseLong(f[3]));
                }
                default -> throw new IllegalArgumentException("unknown record type " + f[0]);
            };
        } catch (IllegalArgumentException | DateTimeParseException | NullPointerException e) {
            // Includes NumberFormatException, enum valueOf and the request's own validation.
            throw new IllegalStateException("Corrupt journal record " + recordNumber + ": \"" + record + "\"", e);
        }
    }

    private static void requireFieldCount(String[] fields, int expected) {
        if (fields.length != expected) {
            throw new IllegalArgumentException("expected " + expected + " fields, found " + fields.length);
        }
    }
}
