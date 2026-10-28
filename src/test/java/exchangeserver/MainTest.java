package exchangeserver;

import exchangeserver.core.Exchange;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import exchangeserver.testing.MutableClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {

    @TempDir
    Path dir;

    private Throwable startupFailure(Path journal) {
        try {
            Exchange.open(journal, MutableClock.at("2026-01-01T10:00:00Z")).close();
            return null;
        } catch (IOException | RuntimeException e) {
            return e;
        }
    }

    // L2: the quarantine advice is printed only when it fits.
    @Test
    void aCorruptJournalGetsNoPoisonPillAdvice() throws IOException {
        Path journal = dir.resolve("x.journal");
        Files.writeString(journal, "hello\n", UTF_8);
        assertEquals("The journal was not changed.", Main.hint(startupFailure(journal)));
    }

    @Test
    void aReplayDivergenceGetsTheCompactionAdvice() throws IOException {
        Path journal = dir.resolve("x.journal");
        try (Exchange ex = Exchange.open(journal, MutableClock.at("2026-01-01T10:00:00Z"))) {
            ex.submit(OrderRequest.limit("a", "1", Side.BUY, 100, 1));
        }
        // A CHECK that no build could reproduce: as if the code that wrote it behaved differently.
        String check = "CHECK,2,2026-01-01T10:00:00Z,0123456789abcdef";
        List<String> lines = Files.readAllLines(journal, UTF_8);
        Files.writeString(journal, String.join("\n", lines) + "\n" + check + "," + exchangeserver.journal.Checksums.crcHex(check) + "\n", UTF_8);
        assertTrue(Main.hint(startupFailure(journal)).contains("--compact"));
    }
}
