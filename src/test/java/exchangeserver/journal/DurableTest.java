package exchangeserver.journal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DurableTest {

    @TempDir
    Path dir;

    @Test
    void writeAtomicallyReplacesTheWholeFileAndLeavesNoTemporaryFile() throws IOException {
        Path target = dir.resolve("f");
        Durable.writeAtomically(target, "old content that is longer\n");
        Durable.writeAtomically(target, "new\n");
        assertEquals("new\n", Files.readString(target, UTF_8));
        assertFalse(Files.exists(dir.resolve("f.tmp")));
    }

    @Test
    void copyDurablyCopies() throws IOException {
        Path source = dir.resolve("a");
        Files.writeString(source, "x\ny\n", UTF_8);
        Durable.copyDurably(source, dir.resolve("b"));
        assertEquals("x\ny\n", Files.readString(dir.resolve("b"), UTF_8));
    }

    @Test
    void fsyncingTheDirectoryIsHarmlessOnEveryPlatform() throws IOException {
        Durable.fsyncDirectory(dir.resolve("anything"));        // Windows ignores it; Linux forces the directory
    }
}
