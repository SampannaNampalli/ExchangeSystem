package exchangeserver.journal;

import exchangeserver.command.Command;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/*

The write-ahead input log. Format in JournalCodec.

Durability  appendAll() returns only after its records are forced to disk: one write and one fsync for the
            whole batch (D37, D52). The caller must not process a command before that. A new journal's directory
            entry is fsynced too (D69).
Integrity   Every record carries a CRC-32C; a record whose checksum does not match is corrupt (D69).
Recovery    open() cuts a torn last record (bytes after the last '\n') and fsyncs the cut (D38). A complete
            record that does not parse is never dropped: reading fails with the record number.
Version     The header names the format, the Rules version the journal was written under, and the base sequence
            (the snapshot it continues from, D71). open() refuses a journal written under other rules (D46): replaying
            it would silently build a different book. A journal with no records yet adopts the new rules: that is
            how a rules change is crossed, after compacting with the old build (D71). The header is checked before
            anything is changed, so a refused file is never modified (D64).
Exclusive   open() holds a JournalLock: a second open, from this process or another, is refused (D65).
Fail-stop   After a failed write or fsync, every later append is refused until restart (D39).

Not thread-safe: one writer thread appends. read() opens its own stream, so it can run while the channel is
open (replay during a poison-pill rebuild does this).

 */
public final class CommandJournal implements AutoCloseable {

    // Diagnostics: how many journals are open in this JVM. Tests use it to prove a handle was released. (Deleting
    // the file proves nothing: Java opens files on Windows with delete sharing, so an open file can be deleted.)
    private static final AtomicInteger OPEN = new AtomicInteger();

    private record FirstRecord(String text, boolean complete, long fileSize) {
    }

    private final Path file;
    private final FileChannel channel;
    private final JournalLock lock;         // null when the caller holds the lock
    private final long baseSequence;
    private final long truncatedBytesOnOpen;
    private final int adoptedRulesFrom;     // the old rules version of an empty journal that adopted ours, or 0
    private boolean failed;

    private CommandJournal(Path file, FileChannel channel, JournalLock lock, long baseSequence, long truncatedBytesOnOpen,
                           int adoptedRulesFrom) {
        this.file = file;
        this.channel = channel;
        this.lock = lock;
        this.baseSequence = baseSequence;
        this.truncatedBytesOnOpen = truncatedBytesOnOpen;
        this.adoptedRulesFrom = adoptedRulesFrom;
    }

    // Opens the journal, creating it with a header if it is missing or empty. Lock, then check, then change:
    // nothing is written to a file this journal does not own (D64, D65).
    public static CommandJournal open(Path file, int rulesVersion) throws IOException {
        Objects.requireNonNull(file, "file");
        JournalLock lock = JournalLock.acquire(file);
        try {
            return openLocked(file, rulesVersion, lock);
        } catch (IOException | RuntimeException e) {
            try {
                lock.close();
            } catch (IOException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }
    }

    // For a caller that holds the journal's lock itself (compaction): closing this journal leaves it held.
    public static CommandJournal open(Path file, int rulesVersion, JournalLock heldLock) throws IOException {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(heldLock, "heldLock");
        return openLocked(file, rulesVersion, null);
    }

    // ownedLock: released by close(), or null if the caller holds the lock.
    private static CommandJournal openLocked(Path file, int rulesVersion, JournalLock ownedLock) throws IOException {
        JournalCodec.Header header = checkHeader(file, rulesVersion);
        int adoptedFrom = 0;
        if (header != null && header.rulesVersion() != rulesVersion) {
            adoptedFrom = header.rulesVersion();            // checkHeader allowed it: no records yet
            writeFresh(file, rulesVersion, header.baseSequence());
            header = new JournalCodec.Header(rulesVersion, header.baseSequence());
        }

        FileChannel channel = FileChannel.open(file, CREATE, WRITE, APPEND);    // APPEND: never overwrite history
        long truncated;
        try {
            truncated = cutTornTail(file);
            if (channel.size() == 0) {
                channel.write(ByteBuffer.wrap(JournalCodec.encodeHeader(rulesVersion, 0).getBytes(UTF_8)));
                channel.force(true);
                Durable.fsyncDirectory(file);           // a new file's directory entry must be durable too
                header = new JournalCodec.Header(rulesVersion, 0);
            }
        } catch (IOException | RuntimeException e) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        OPEN.incrementAndGet();
        return new CommandJournal(file, channel, ownedLock, header.baseSequence(), truncated, adoptedFrom);
    }

    // Replaces `file` with a journal that holds only a header (compaction, D71). The caller must hold the lock.
    public static void writeFresh(Path file, int rulesVersion, long baseSequence) throws IOException {
        Durable.writeAtomically(file, JournalCodec.encodeHeader(rulesVersion, baseSequence));
    }

    public static int openJournals() {
        return OPEN.get();
    }

    public void append(Command command) throws IOException {
        appendAll(List.of(command));
    }

    // All or none are encoded before anything is written: a command that cannot be encoded writes nothing.
    public void appendAll(List<? extends Command> commands) throws IOException {
        if (failed) {
            throw new IllegalStateException("Journal failed earlier; restart to recover");
        }
        if (commands.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder(commands.size() * 112);
        for (Command command : commands) {
            sb.append(JournalCodec.encode(command));
        }
        ByteBuffer records = ByteBuffer.wrap(sb.toString().getBytes(UTF_8));

        try {
            while (records.hasRemaining()) {
                channel.write(records);
            }
            channel.force(true);
        } catch (IOException | RuntimeException e) {
            // A partial record may be on disk, and a failed fsync cannot be retried safely.
            failed = true;
            throw e;
        }
    }

    // Streams every command, oldest first, header excluded. Memory stays constant whatever the journal size.
    public void read(Consumer<? super Command> consumer) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream(128);
        long recordNumber = 0;
        byte[] buffer = new byte[64 * 1024];

        try (InputStream in = new BufferedInputStream(Files.newInputStream(file), buffer.length)) {
            int n;
            while ((n = in.read(buffer)) != -1) {
                int start = 0;
                for (int i = 0; i < n; i++) {
                    if (buffer[i] == JournalCodec.END_OF_RECORD) {
                        record.write(buffer, start, i - start);
                        recordNumber++;
                        String text = record.toString(UTF_8);
                        if (recordNumber == 1) {
                            JournalCodec.decodeHeader(text);        // validated by open(); skipped here
                        } else {
                            consumer.accept(JournalCodec.decode(text, recordNumber));
                        }
                        record.reset();
                        start = i + 1;
                    }
                }
                record.write(buffer, start, n - start);
            }
        }
        // Read bytes, not lines: readLine() cannot tell a finished last record from a cut-off one.
        if (record.size() > 0) {
            throw new IllegalStateException("Journal record " + (recordNumber + 1) + " has no end of record: \""
                    + record.toString(UTF_8) + "\"");
        }
    }

    // The last sequence number of the snapshot this journal continues from; 0 for none (D71).
    public long baseSequence() {
        return baseSequence;
    }

    public boolean hasFailed() {
        return failed;
    }

    public long getTruncatedBytesOnOpen() {
        return truncatedBytesOnOpen;
    }

    // The rules version an empty journal was written under before it adopted this build's, or 0 (D71).
    public int adoptedRulesFrom() {
        return adoptedRulesFrom;
    }

    // Idempotent. Releases the lock if this journal took it.
    @Override
    public synchronized void close() throws IOException {
        if (channel.isOpen()) {
            OPEN.decrementAndGet();
            try {
                channel.close();
            } finally {
                if (lock != null) {
                    lock.close();
                }
            }
        }
    }

    // ---------- Recovery ----------

    // Refuses, before anything is cut or written, a file that is not a journal written under these rules (D64).
    // A file with no complete record may only be the start of a header (a crash while the journal was being
    // created). Returns the header, or null for a missing, empty or torn-header file. A journal written under
    // other rules is accepted only if it has no records yet (D71); the caller rewrites its header.
    private static JournalCodec.Header checkHeader(Path file, int rulesVersion) throws IOException {
        if (!Files.exists(file)) {
            return null;
        }
        FirstRecord first = readFirstRecord(file);
        if (!first.complete()) {
            if (!JournalCodec.encodeHeader(rulesVersion, 0).startsWith(first.text())) {
                throw new IllegalStateException("Journal " + file + " does not start with a journal header: \"" + first.text() + "\"");
            }
            return null;
        }
        JournalCodec.Header header = JournalCodec.decodeHeader(first.text());
        boolean noRecords = first.fileSize() == first.text().getBytes(UTF_8).length + 1;
        if (header.rulesVersion() != rulesVersion && !noRecords) {
            throw new IllegalStateException("Journal " + file + " was written under rules version " + header.rulesVersion()
                    + "; this build uses " + rulesVersion + ". Compact it with the build that wrote it (--compact); "
                    + "this build can then start from the compacted journal and its snapshot.");
        }
        return header;
    }

    // Cuts everything after the last '\n' and returns how many bytes were cut.
    private static long cutTornTail(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, CREATE, READ, WRITE)) {
            long size = ch.size();
            long end = endOfLastCompleteRecord(ch, size);
            if (end < size) {
                ch.truncate(end);
                ch.force(true);                 // the cut must be durable before new records follow it
            }
            return size - end;
        }
    }

    // Position just after the last '\n', or 0 if there is none. Scans backwards in blocks, so recovery costs
    // the length of the last record, not of the file.
    private static long endOfLastCompleteRecord(FileChannel ch, long size) throws IOException {
        ByteBuffer block = ByteBuffer.allocate(4096);
        long blockEnd = size;
        while (blockEnd > 0) {
            int length = (int) Math.min(block.capacity(), blockEnd);
            long blockStart = blockEnd - length;
            block.clear().limit(length);
            while (block.hasRemaining()) {
                if (ch.read(block, blockStart + block.position()) < 0) {
                    throw new IOException("Journal shrank while being recovered");
                }
            }
            for (int i = length - 1; i >= 0; i--) {
                if (block.get(i) == JournalCodec.END_OF_RECORD) {
                    return blockStart + i + 1;
                }
            }
            blockEnd = blockStart;
        }
        return 0;
    }

    private static FirstRecord readFirstRecord(Path file) throws IOException {
        ByteArrayOutputStream record = new ByteArrayOutputStream(64);
        boolean complete = false;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
            int b;
            while ((b = in.read()) != -1) {
                if (b == JournalCodec.END_OF_RECORD) {
                    complete = true;
                    break;
                }
                record.write(b);
                if (record.size() > 1024) {
                    break;                      // no header is this long: incomplete, and no header's prefix
                }
            }
        }
        return new FirstRecord(record.toString(UTF_8), complete, Files.size(file));
    }
}
