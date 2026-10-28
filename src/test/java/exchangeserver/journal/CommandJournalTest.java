package exchangeserver.journal;

import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.command.SkipCommand;
import exchangeserver.domain.OrderRequest;
import exchangeserver.domain.Side;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandJournalTest {

    private static final Instant T = Instant.parse("2026-01-01T10:00:00Z");
    private static final int RULES = 1;

    @TempDir
    Path dir;

    private Path file() {
        return dir.resolve("test.journal");
    }

    private static Command order(long seq, long id) {
        return new NewOrderCommand(seq, T, id, OrderRequest.limit("a", "c" + id, Side.BUY, 100, 1));
    }

    private static List<Command> readAll(CommandJournal journal) throws IOException {
        List<Command> commands = new ArrayList<>();
        journal.read(commands::add);
        return commands;
    }

    private String text() throws IOException {
        return Files.readString(file(), UTF_8);
    }

    @Nested
    class Header {

        @Test
        void newJournalStartsWithAHeader() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                assertEquals(JournalCodec.encodeHeader(RULES, 0), text());
                assertTrue(readAll(j).isEmpty());
            }
        }

        @Test
        void reopeningDoesNotWriteASecondHeader() throws IOException {
            CommandJournal.open(file(), RULES).close();
            CommandJournal.open(file(), RULES).close();
            assertEquals(JournalCodec.encodeHeader(RULES, 0), text());
        }

        @Test
        void otherRulesVersionIsRefusedAndTheFileIsReleased() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(1, 1));
            }
            int open = CommandJournal.openJournals();
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> CommandJournal.open(file(), RULES + 1));
            assertTrue(e.getMessage().contains("rules version"), e.getMessage());
            assertEquals(open, CommandJournal.openJournals(), "the refused journal's channel was closed");
            CommandJournal.open(file(), RULES).close();     // the refused open released its lock (D65)
        }

        // D71: the way across a rules change is a compacted, empty journal; it takes the new version.
        @Test
        void anEmptyJournalAdoptsANewRulesVersionAndKeepsItsBase() throws IOException {
            CommandJournal.writeFresh(file(), RULES, 9);
            try (CommandJournal j = CommandJournal.open(file(), RULES + 1)) {
                assertEquals(RULES, j.adoptedRulesFrom());
                assertEquals(9, j.baseSequence());
            }
            assertEquals(JournalCodec.encodeHeader(RULES + 1, 9), text());
            try (CommandJournal j = CommandJournal.open(file(), RULES + 1)) {
                assertEquals(0, j.adoptedRulesFrom(), "adopted once, then it is simply this build's journal");
            }
        }

        @Test
        void fileWithoutAHeaderIsRefused() throws IOException {
            Files.writeString(file(), "NEW,1,2026-01-01T10:00:00Z,1,a,c,BUY,LIMIT,1,100\n", UTF_8);
            int open = CommandJournal.openJournals();
            assertThrows(IllegalStateException.class, () -> CommandJournal.open(file(), RULES));
            assertEquals(open, CommandJournal.openJournals());
        }

        // D64: open() used to cut the "torn tail" of any file before checking its header.
        @Test
        void fileThatIsNotAJournalIsRefusedUnchanged() throws IOException {
            Files.writeString(file(), "hello\nworld", UTF_8);
            int open = CommandJournal.openJournals();
            assertThrows(IllegalStateException.class, () -> CommandJournal.open(file(), RULES));
            assertEquals("hello\nworld", text(), "a refused file is never cut");
            assertEquals(open, CommandJournal.openJournals());
        }

        @Test
        void textWithoutAnyRecordIsRefusedUnlessItStartsAHeader() throws IOException {
            Files.writeString(file(), "hello", UTF_8);
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> CommandJournal.open(file(), RULES));
            assertTrue(e.getMessage().contains("does not start with a journal header"), e.getMessage());
            assertEquals("hello", text());
        }

        @Test
        void otherRulesVersionWithATornTailIsRefusedUnchanged() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES + 1)) {
                j.append(order(1, 1));
            }
            Files.writeString(file(), "NEW,2,2026", UTF_8, StandardOpenOption.APPEND);
            String before = text();
            assertThrows(IllegalStateException.class, () -> CommandJournal.open(file(), RULES));
            assertEquals(before, text());
        }

        @Test
        void openAndCloseAreCountedAndCloseIsIdempotent() throws IOException {
            int open = CommandJournal.openJournals();
            CommandJournal j = CommandJournal.open(file(), RULES);
            assertEquals(open + 1, CommandJournal.openJournals());
            assertTrue(!j.hasFailed());
            j.close();
            j.close();
            assertEquals(open, CommandJournal.openJournals());
        }
    }

    @Nested
    class AppendAndRead {

        @Test
        void readsBackInOrder() throws IOException {
            List<Command> written = List.of(order(1, 1), new CancelOrderCommand(2, T, "a", 1), new SkipCommand(3, T, 2));
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                for (Command c : written) {
                    j.append(c);
                }
                assertEquals(written, readAll(j));
            }
        }

        @Test
        void appendAllWritesTheWholeBatch() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.appendAll(List.of(order(1, 1), order(2, 2), order(3, 3)));
                assertEquals(3, readAll(j).size());
            }
        }

        @Test
        void emptyBatchWritesNothing() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.appendAll(List.of());
            }
            assertEquals(JournalCodec.encodeHeader(RULES, 0), text());
        }

        @Test
        void reopeningAppendsAfterExistingRecords() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(1, 1));
            }
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(2, 2));
                assertEquals(List.of(order(1, 1), order(2, 2)), readAll(j));
            }
        }

        @Test
        void recordsUseLfOnly() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(1, 1));
            }
            assertTrue(!text().contains("\r"));
        }

        @Test
        void readHandlesRecordsSpanningBufferBoundaries() throws IOException {
            List<Command> written = new ArrayList<>();
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                for (int i = 1; i <= 5_000; i++) {      // ~350 KB: several 64 KB buffers
                    written.add(order(i, i));
                }
                j.appendAll(written);
                assertEquals(written, readAll(j));
            }
        }

        @Test
        void encodeFailureWritesNothing() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                List<Command> withNull = new ArrayList<>();
                withNull.add(order(1, 1));
                withNull.add(null);
                assertThrows(NullPointerException.class, () -> j.appendAll(withNull));
            }
            assertEquals(JournalCodec.encodeHeader(RULES, 0), text());
        }
    }

    @Nested
    class Recovery {

        @Test
        void tornLastRecordIsCutOnOpen() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(1, 1));
            }
            Files.writeString(file(), "NEW,2,2026-01-01T10:0", UTF_8, StandardOpenOption.APPEND);
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                assertEquals(21, j.getTruncatedBytesOnOpen());
                assertEquals(List.of(order(1, 1)), readAll(j));
                j.append(order(2, 2));
                assertEquals(2, readAll(j).size(), "new records follow the cut cleanly");
            }
        }

        @Test
        void tornHeaderIsCutAndRewritten() throws IOException {
            Files.writeString(file(), "HEAD", UTF_8);
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                assertEquals(4, j.getTruncatedBytesOnOpen());
            }
            assertEquals(JournalCodec.encodeHeader(RULES, 0), text());
        }

        @Test
        void cleanFileCutsNothing() throws IOException {
            CommandJournal.open(file(), RULES).close();
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                assertEquals(0, j.getTruncatedBytesOnOpen());
            }
        }

        @Test
        void completeCorruptRecordIsNeverDropped() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(1, 1));
            }
            Files.writeString(file(), "NEW,garbage\n", UTF_8, StandardOpenOption.APPEND);
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                assertEquals(0, j.getTruncatedBytesOnOpen());
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> readAll(j));
                assertTrue(e.getMessage().contains("record 3"), e.getMessage());
            }
        }

        @Test
        void unterminatedRecordFoundByReadIsAnError() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                // Simulates a write that appears after open(): read() must not treat it as complete.
                Files.writeString(file(), "CANCEL,1,2026-01-01T10:00:00Z,a,1", UTF_8, StandardOpenOption.APPEND);
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> readAll(j));
                assertTrue(e.getMessage().contains("record 2 has no end of record"), e.getMessage());
            }
        }

        @Test
        void longTornTailAcrossScanBlocksIsCut() throws IOException {
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                j.append(order(1, 1));
            }
            Files.writeString(file(), "x".repeat(10_000), UTF_8, StandardOpenOption.APPEND);
            try (CommandJournal j = CommandJournal.open(file(), RULES)) {
                assertEquals(10_000, j.getTruncatedBytesOnOpen());
                assertEquals(1, readAll(j).size());
            }
        }
    }

    // D65: nothing stopped a second writer (e.g. --quarantine against a running exchange) from appending.
    @Nested
    class Exclusive {

        @Test
        void aSecondOpenIsRefusedUntilTheFirstCloses() throws IOException {
            try (CommandJournal first = CommandJournal.open(file(), RULES)) {
                int open = CommandJournal.openJournals();
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> CommandJournal.open(file(), RULES));
                assertTrue(e.getMessage().contains("already open"), e.getMessage());
                assertEquals(open, CommandJournal.openJournals(), "the refused channel was closed");

                first.append(order(1, 1));          // the refusal did not disturb the owner
                assertEquals(List.of(order(1, 1)), readAll(first));
            }
            try (CommandJournal again = CommandJournal.open(file(), RULES)) {
                assertEquals(1, readAll(again).size());
            }
        }

        // The lock must hold against another process even after this one has opened and closed other handles on
        // the journal (read() does): on Linux and macOS that closing drops POSIX locks on the journal file itself.
        @Test
        void anotherProcessIsRefusedWhileTheJournalIsOpen() throws Exception {
            try (CommandJournal held = CommandJournal.open(file(), RULES)) {
                held.append(order(1, 1));
                readAll(held);
                String java = ProcessHandle.current().info().command().orElse("java");
                Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                        "exchangeserver.Main", "--journal", file().toString(), "--quarantine", "1")
                        .redirectErrorStream(true)
                        .start();
                assertTrue(child.waitFor(60, TimeUnit.SECONDS), "the child JVM did not finish");
                String output = new String(child.getInputStream().readAllBytes(), UTF_8);
                assertEquals(1, child.exitValue(), output);
                assertTrue(output.contains("already open"), output);
            }
            assertEquals(2, Files.readAllLines(file(), UTF_8).size(), "header and one order: no SKIP was appended");
        }
    }

    @Nested
    class FailStop {

        @Test
        void afterAFailedWriteEveryAppendIsRefused() throws IOException {
            CommandJournal j = CommandJournal.open(file(), RULES);
            j.close();                                  // the next write fails with ClosedChannelException
            assertThrows(IOException.class, () -> j.append(order(1, 1)));
            assertTrue(j.hasFailed());
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> j.append(order(1, 1)));
            assertTrue(e.getMessage().contains("restart"), e.getMessage());
        }
    }
}
