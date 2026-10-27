package exchangeserver.command;

import java.time.Instant;
import java.util.Objects;
import java.util.regex.Pattern;

/*

A checkpoint (D70): "the digest of every engine output for the commands before this one is `digest`". Written
by the live exchange every few thousand commands; replay recomputes the digest and stops if it differs, so a code
change that makes old commands behave differently is caught instead of silently building a different book.

Not an engine input: replay compares it, never feeds it.

 */
public record CheckCommand(long sequenceNumber, Instant timestamp, String digest) implements Command {

    private static final Pattern HEX16 = Pattern.compile("[0-9a-f]{16}");

    public CheckCommand {
        Objects.requireNonNull(timestamp, "timestamp");
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("Sequence number must be positive");
        }
        if (digest == null || !HEX16.matcher(digest).matches()) {
            throw new IllegalArgumentException("Digest must be 16 lower-case hex digits: " + digest);
        }
    }
}
