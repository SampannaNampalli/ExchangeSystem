package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.book.LevelUpdate;
import exchangeserver.engine.CancelAllOutput;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.Execution;
import exchangeserver.engine.OrderOutput;
import exchangeserver.engine.Response;
import exchangeserver.engine.Trade;

import static java.nio.charset.StandardCharsets.UTF_8;

/*

A running digest of every engine output (D70): FNV-1a 64 over a canonical text encoding of each output, in
order. If replay produces any different output for the same commands (a reject that was a fill, a different
price, one level update more), the digest differs from the one the live run wrote in a CHECK record.

The encoding is spelled out field by field rather than taken from toString() or hashCode(), whose formats Java
does not promise to keep. Not a cryptographic hash: it detects accidents, not attacks.

 */
final class OutputDigest {

    private static final long OFFSET = 0xcbf29ce484222325L;
    private static final long PRIME = 0x100000001b3L;

    private long state;

    OutputDigest() {
        this(OFFSET);
    }

    private OutputDigest(long state) {
        this.state = state;
    }

    static OutputDigest resume(String hex) {
        return new OutputDigest(Long.parseUnsignedLong(hex, 16));
    }

    String hex() {
        return String.format("%016x", state);
    }

    void update(EngineOutput output) {
        StringBuilder sb = new StringBuilder(128);
        sb.append(output.sequenceNumber()).append('|').append(output.timestamp()).append('|');
        if (output instanceof OrderOutput o) {
            Response r = o.response();
            sb.append("O|").append(r.orderId()).append('|').append(r.clientOrderId()).append('|').append(r.status())
                    .append('|').append(r.reason()).append('|').append(r.filledQuantity()).append('|')
                    .append(r.remainingQuantity());
            for (Trade t : r.trades()) {
                sb.append("|T").append(t.price()).append(',').append(t.quantity()).append(',').append(t.buyOrderId())
                        .append(',').append(t.sellOrderId()).append(',').append(t.aggressorSide());
            }
            for (Execution e : o.passiveFills()) {
                sb.append("|E").append(e.participant()).append(',').append(e.orderId()).append(',').append(e.price())
                        .append(',').append(e.quantity()).append(',').append(e.leavesQuantity());
            }
        } else if (output instanceof CancelOutput c) {
            sb.append("C|");
            append(sb, c.result());
        } else if (output instanceof CancelAllOutput a) {
            sb.append("A|").append(a.participant());
            for (CancelResult r : a.results()) {
                sb.append('|');
                append(sb, r);
            }
        }
        for (LevelUpdate l : output.levelUpdates()) {
            sb.append("|L").append(l.side()).append(',').append(l.price()).append(',').append(l.volume())
                    .append(',').append(l.orderCount());
        }
        sb.append('\n');
        for (byte b : sb.toString().getBytes(UTF_8)) {
            state ^= b & 0xff;
            state *= PRIME;
        }
    }

    private static void append(StringBuilder sb, CancelResult r) {
        sb.append(r.status()).append(',').append(r.orderId()).append(',').append(r.cancelledQuantity());
    }
}
