package exchangeserver.core;

import exchangeserver.book.RestingOrderView;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;
import exchangeserver.engine.EngineState;
import exchangeserver.journal.Checksums;
import exchangeserver.journal.Durable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/*

The exchange's state after command `lastSequence` (D71): what replay would have built from every command up to
it, so recovery can start here instead of at sequence 1. Written by compaction (Exchange.compact), read when the
journal's header names lastSequence as its base.

File "<journal>.snapshot-<lastSequence>", one record per line, each ending in ",<crc>" like the journal (D69):

    SNAPSHOT,<format>,<lastSequence>,<lastOrderId>,<lastTimestamp or ->,<lastTradePrice>,<digest>
    ORDER,<orderId>,<participant>,<clientOrderId>,<side>,<type>,<price>,<originalQty>,<filledQty>,<timestamp>
    OSEQ,<participant>,<lastOseq>
    END,<orderCount>,<participantCount>

ORDER lines are in book priority (bids best first, then asks best first, time priority within a level), so
loading them in order restores time priority. The END line proves the file is complete. A snapshot does not
depend on Rules: it is state, not commands to re-run, which is what lets a rules change be crossed.

 */
record Snapshot(long lastSequence, long lastOrderId, Instant lastTimestamp, long lastTradePrice, String digest,
                List<RestingOrderView> orders, Map<String, Long> lastOseqs) {

    static final int FORMAT_VERSION = 1;

    Snapshot {
        Objects.requireNonNull(digest, "digest");
        orders = List.copyOf(orders);
        lastOseqs = Map.copyOf(lastOseqs);
    }

    static Snapshot empty() {
        return new Snapshot(0, 0, null, 0, new OutputDigest().hex(), List.of(), Map.of());
    }

    EngineState engineState() {
        return new EngineState(lastSequence, lastOrderId, lastTradePrice, orders);
    }

    static Path path(Path journal, long lastSequence) {
        return journal.resolveSibling(journal.getFileName() + ".snapshot-" + lastSequence);
    }

    void write(Path file) throws IOException {
        StringBuilder sb = new StringBuilder();
        line(sb, "SNAPSHOT," + FORMAT_VERSION + "," + lastSequence + "," + lastOrderId + ","
                + (lastTimestamp == null ? "-" : lastTimestamp.toString()) + "," + lastTradePrice + "," + digest);
        for (RestingOrderView o : orders) {
            line(sb, "ORDER," + o.orderId() + "," + o.participant() + "," + o.clientOrderId() + "," + o.side() + ","
                    + o.orderType() + "," + o.price() + "," + o.originalQuantity() + "," + o.filledQuantity() + ","
                    + o.timestamp());
        }
        Map<String, Long> sorted = new TreeMap<>(lastOseqs);
        sorted.forEach((p, n) -> line(sb, "OSEQ," + p + "," + n));
        line(sb, "END," + orders.size() + "," + sorted.size());
        Durable.writeAtomically(file, sb.toString());
    }

    static Snapshot read(Path file) throws IOException {
        if (!Files.exists(file)) {
            throw new IllegalStateException("Snapshot " + file + " is missing: the journal continues from it");
        }
        List<String> lines = Files.readAllLines(file, UTF_8);
        int n = 0;
        try {
            String[] h = fields(lines, n++, "SNAPSHOT", 7);
            if (Integer.parseInt(h[1]) != FORMAT_VERSION) {
                throw new IllegalArgumentException("format version " + h[1] + ", this code reads " + FORMAT_VERSION);
            }
            long lastSequence = Long.parseLong(h[2]);
            long lastOrderId = Long.parseLong(h[3]);
            Instant lastTimestamp = h[4].equals("-") ? null : Instant.parse(h[4]);
            long lastTradePrice = Long.parseLong(h[5]);
            String digest = h[6];
            OutputDigest.resume(digest);                // validates the hex

            List<RestingOrderView> orders = new ArrayList<>();
            while (n < lines.size() && lines.get(n).startsWith("ORDER,")) {
                String[] o = fields(lines, n++, "ORDER", 10);
                orders.add(new RestingOrderView(Long.parseLong(o[1]), o[2], o[3], Side.valueOf(o[4]), OrderType.valueOf(o[5]),
                        Long.parseLong(o[6]), Long.parseLong(o[7]), Long.parseLong(o[8]), Instant.parse(o[9])));
            }
            Map<String, Long> lastOseqs = new TreeMap<>();
            while (n < lines.size() && lines.get(n).startsWith("OSEQ,")) {
                String[] s = fields(lines, n++, "OSEQ", 3);
                lastOseqs.put(s[1], Long.parseLong(s[2]));
            }
            String[] end = fields(lines, n++, "END", 3);
            if (Long.parseLong(end[1]) != orders.size() || Long.parseLong(end[2]) != lastOseqs.size() || n != lines.size()) {
                throw new IllegalArgumentException("END does not match the records before it");
            }
            return new Snapshot(lastSequence, lastOrderId, lastTimestamp, lastTradePrice, digest, orders, lastOseqs);
        } catch (IllegalArgumentException | DateTimeParseException | IndexOutOfBoundsException e) {
            throw new IllegalStateException("Corrupt snapshot " + file + " at line " + n + ": " + e.getMessage(), e);
        }
    }

    private static void line(StringBuilder sb, String text) {
        sb.append(text).append(',').append(Checksums.crcHex(text)).append('\n');
    }

    private static String[] fields(List<String> lines, int index, String type, int count) {
        if (index >= lines.size()) {
            throw new IllegalArgumentException("missing " + type + " record (file truncated?)");
        }
        String record = lines.get(index);
        int comma = record.lastIndexOf(',');
        if (comma < 0 || !Checksums.crcHex(record.substring(0, comma)).equals(record.substring(comma + 1))) {
            throw new IllegalArgumentException("checksum mismatch");
        }
        String[] f = record.substring(0, comma).split(",", -1);
        if (!f[0].equals(type) || f.length != count) {
            throw new IllegalArgumentException("expected a " + type + " record with " + count + " fields");
        }
        return f;
    }
}
