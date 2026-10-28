package exchangeserver.journal;

import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.CheckCommand;
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

The journal's text format (D36, D69), one record per line, ','-separated, every record ending in
",<crc>\n" where <crc> is the CRC-32C of the record's text before that last comma, as 8 lower-case hex digits:

    HEADER,<formatVersion>,<rulesVersion>,<baseSequence>                                    (first record only)
    NEW,<seq>,<timestamp>,<orderId>,<participant>,<clientOrderId>,<side>,<type>,<quantity>,<price>
    CANCEL,<seq>,<timestamp>,<participant>,<targetOrderId>
    CANCEL_ALL,<seq>,<timestamp>,<participant>
    SKIP,<seq>,<timestamp>,<skippedSeq>
    CHECK,<seq>,<timestamp>,<digest>

baseSequence: the last sequence number of the snapshot this journal continues from (0: from an empty exchange,
D71); the first record after the header has sequence baseSequence + 1. A MARKET order's price field is "-" (D26).
Enums by name(); timestamps by Instant.toString(). Participant and client order id cannot contain ',' (Rules).

Decoding rebuilds requests through OrderRequest's factories, so the journal applies the same stateless rules as
the gateway (D19). Anything that does not parse, or whose checksum does not match, is corrupt journal data:
IllegalStateException (D27).

 */
final class JournalCodec {

    static final int FORMAT_VERSION = 3;
    static final String HEADER = "HEADER";
    static final String NEW = "NEW";
    static final String CANCEL = "CANCEL";
    static final String CANCEL_ALL = "CANCEL_ALL";
    static final String SKIP = "SKIP";
    static final String CHECK = "CHECK";
    static final String NO_PRICE = "-";
    static final char SEPARATOR = ',';
    static final char END_OF_RECORD = '\n';

    record Header(int rulesVersion, long baseSequence) {
    }

    private JournalCodec() {
    }

    static String encodeHeader(int rulesVersion, long baseSequence) {
        return seal(HEADER + SEPARATOR + FORMAT_VERSION + SEPARATOR + rulesVersion + SEPARATOR + baseSequence);
    }

    // Throws if this is not a header this code can read.
    static Header decodeHeader(String record) {
        try {
            String[] f = unseal(record).split(String.valueOf(SEPARATOR), -1);
            if (!f[0].equals(HEADER)) {
                throw new IllegalArgumentException("first record is not a header");
            }
            if (f.length < 2) {
                throw new IllegalArgumentException("no format version");
            }
            int format = Integer.parseInt(f[1]);
            if (format != FORMAT_VERSION) {
                throw new IllegalArgumentException("format version " + format + ", this code reads " + FORMAT_VERSION);
            }
            requireFieldCount(f, 4);
            long base = Long.parseLong(f[3]);
            if (base < 0) {
                throw new IllegalArgumentException("negative base sequence");
            }
            return new Header(Integer.parseInt(f[2]), base);
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
        } else if (command instanceof CancelAllCommand c) {
            sb.append(CANCEL_ALL)
                    .append(SEPARATOR).append(c.sequenceNumber())
                    .append(SEPARATOR).append(c.timestamp())
                    .append(SEPARATOR).append(c.participant());
        } else if (command instanceof SkipCommand s) {
            sb.append(SKIP)
                    .append(SEPARATOR).append(s.sequenceNumber())
                    .append(SEPARATOR).append(s.timestamp())
                    .append(SEPARATOR).append(s.skippedSequenceNumber());
        } else if (command instanceof CheckCommand c) {
            sb.append(CHECK)
                    .append(SEPARATOR).append(c.sequenceNumber())
                    .append(SEPARATOR).append(c.timestamp())
                    .append(SEPARATOR).append(c.digest());
        } else {
            throw new IllegalStateException("Unknown command type " + command.getClass());
        }
        return seal(sb.toString());
    }

    // record: one record without its '\n'. recordNumber: 1-based, header included, for the error message.
    static Command decode(String record, long recordNumber) {
        try {
            String[] f = unseal(record).split(String.valueOf(SEPARATOR), -1);     // -1 keeps trailing empty fields
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
                case CANCEL_ALL -> {
                    requireFieldCount(f, 4);
                    yield new CancelAllCommand(Long.parseLong(f[1]), Instant.parse(f[2]), f[3]);
                }
                case SKIP -> {
                    requireFieldCount(f, 4);
                    yield new SkipCommand(Long.parseLong(f[1]), Instant.parse(f[2]), Long.parseLong(f[3]));
                }
                case CHECK -> {
                    requireFieldCount(f, 4);
                    yield new CheckCommand(Long.parseLong(f[1]), Instant.parse(f[2]), f[3]);
                }
                default -> throw new IllegalArgumentException("unknown record type " + f[0]);
            };
        } catch (IllegalArgumentException | DateTimeParseException | NullPointerException e) {
            // Includes NumberFormatException, enum valueOf, checksum failures and the request's own validation.
            throw new IllegalStateException("Corrupt journal record " + recordNumber + ": \"" + record + "\"", e);
        }
    }

    // Appends ",<crc>" and the end of record.
    static String seal(String text) {
        return text + SEPARATOR + Checksums.crcHex(text) + END_OF_RECORD;
    }

    // The record's text without its checksum; throws if the checksum is missing or wrong.
    static String unseal(String record) {
        if (record.endsWith("\r")) {
            // D69: the decoder reads LF-terminated records; say so instead of a puzzling checksum error.
            throw new IllegalArgumentException("record ends with '\\r': the file was converted to CRLF line endings; "
                    + "convert it back to LF (e.g. dos2unix) and restart");
        }
        int comma = record.lastIndexOf(SEPARATOR);
        if (comma < 0) {
            throw new IllegalArgumentException("no checksum");
        }
        String text = record.substring(0, comma);
        String crc = record.substring(comma + 1);
        if (!Checksums.crcHex(text).equals(crc)) {
            throw new IllegalArgumentException("checksum mismatch (expected " + Checksums.crcHex(text) + ", found " + crc + ")");
        }
        return text;
    }

    private static void requireFieldCount(String[] fields, int expected) {
        if (fields.length != expected) {
            throw new IllegalArgumentException("expected " + expected + " fields, found " + fields.length);
        }
    }
}
