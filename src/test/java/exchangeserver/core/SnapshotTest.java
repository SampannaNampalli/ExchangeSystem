package exchangeserver.core;

import exchangeserver.book.RestingOrderView;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;
import exchangeserver.journal.Checksums;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");

    @TempDir
    Path dir;

    private static Snapshot sample() {
        return new Snapshot(12, 9, T, 100, "0011223344556677",
                List.of(new RestingOrderView(3, "a", "c3", Side.BUY, OrderType.LIMIT, 99, 5, 2, T),
                        new RestingOrderView(7, "b", "c7", Side.SELL, OrderType.POST_ONLY, 101, 1, 0, T.plusSeconds(1))),
                Map.of("b", 4L, "a", 9L));
    }

    @Test
    void roundTrip() throws IOException {
        Path file = Snapshot.path(dir.resolve("x.journal"), 12);
        assertEquals("x.journal.snapshot-12", file.getFileName().toString());
        sample().write(file);
        assertEquals(sample(), Snapshot.read(file));
        assertTrue(Files.readString(file, UTF_8).contains("OSEQ,a,9"), "participants sorted");
    }

    @Test
    void anyChangedCharacterIsCaught() throws IOException {
        Path file = dir.resolve("s");
        sample().write(file);
        String text = Files.readString(file, UTF_8).replace(",99,5,2,", ",99,5,1,");
        Files.writeString(file, text, UTF_8);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Snapshot.read(file));
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());     // 1-based: the first ORDER record
    }

    @Test
    void aTruncatedFileIsCaughtByItsEndRecord() throws IOException {
        Path file = dir.resolve("s");
        sample().write(file);
        List<String> lines = Files.readAllLines(file, UTF_8);
        Files.writeString(file, String.join("\n", lines.subList(0, lines.size() - 1)) + "\n", UTF_8);
        assertThrows(IllegalStateException.class, () -> Snapshot.read(file));
    }

    @Test
    void anEndRecordThatDisagreesIsCaught() throws IOException {
        Path file = dir.resolve("s");
        sample().write(file);
        List<String> lines = Files.readAllLines(file, UTF_8);
        String end = "END,1,2";
        lines.set(lines.size() - 1, end + "," + Checksums.crcHex(end));
        Files.writeString(file, String.join("\n", lines) + "\n", UTF_8);
        assertThrows(IllegalStateException.class, () -> Snapshot.read(file));
    }

    @Test
    void aMissingSnapshotIsExplained() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Snapshot.read(dir.resolve("none")));
        assertTrue(e.getMessage().contains("missing"), e.getMessage());
    }

    @Test
    void theEmptySnapshotIsTheStartOfHistory() {
        Snapshot empty = Snapshot.empty();
        assertEquals(0, empty.lastSequence());
        assertEquals(new OutputDigest().hex(), empty.digest());
        assertTrue(empty.orders().isEmpty());
    }
}
