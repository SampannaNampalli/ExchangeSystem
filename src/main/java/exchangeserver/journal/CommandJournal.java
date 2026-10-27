package exchangeserver.journal;

import exchangeserver.command.Command;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
            whole batch (D37, D52). The caller must not process a command before that.
Recovery    open() cuts a torn last record (bytes after the last '\n') and fsyncs the cut (D38). A complete
            record that does not parse is never dropped: reading fails with the record number.
Version     The first record names the format and the Rules version the journal was written under. open()
            refuses a journal written under other rules (D46): replaying it would silently build a different
            book. The header is checked before the cut, so a refused file is never changed (D64).
Exclusive   open() locks "<journal>.lock": a second open, from this process or another (e.g. --quarantine
            against a running exchange), is refused instead of interleaving sequence numbers (D65).
Fail-stop   After a failed write or fsync, every later append is refused until restart (D39).

Not thread-safe: one writer thread appends. read() opens its own stream, so it can run while the channel is
open (replay during a poison-pill rebuild does this).

 */
public final class CommandJournal implements AutoCloseable {

    // Diagnostics: how many journals are open in this JVM. Tests use it to prove a handle was released. (Deleting
    // the file proves nothing: Java opens files on Windows with delete sharing, so an open file can be deleted.)
    private static final AtomicInteger OPEN = new AtomicInteger();

    // Lock files held by journals open in this JVM (D65).
    private static final Set<Path> HELD = ConcurrentHashMap.newKeySet();

    private record FirstRecord(String text, boolean complete) {
    }

    private final Path file;
    private final FileChannel channel;
    private final Lock lock;
    private final long truncatedBytesOnOpen;
    private boolean failed;

    private CommandJournal(Path file, FileChannel channel, Lock lock, long truncatedBytesOnOpen) {
        this.file = file;
        this.channel = channel;
        this.lock = lock;
        this.truncatedBytesOnOpen = truncatedBytesOnOpen;
    }

    // Opens the journal, creating it with a header if it is missing or empty. Lock, then check, then change:
    // nothing is written to a file this journal does not own (D64, D65).
    public static CommandJournal open(Path file, int rulesVersion) throws IOException {
        Objects.requireNonNull(file, "file");
        Lock lock = Lock.acquire(file);
        FileChannel channel = null;
        long truncated;
        try {
            channel = FileChannel.open(file, CREATE, WRITE, APPEND);            // APPEND: never overwrite history
            checkHeader(file, rulesVersion);
            truncated = cutTornTail(file);
            if (channel.size() == 0) {
                channel.write(ByteBuffer.wrap(JournalCodec.encodeHeader(rulesVersion).getBytes(UTF_8)));
                channel.force(true);
            }
        } catch (IOException | RuntimeException e) {
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            try {
                lock.release();
            } catch (IOException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw e;
        }
        OPEN.incrementAndGet();
        return new CommandJournal(file, channel, lock, truncated);
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
        StringBuilder sb = new StringBuilder(commands.size() * 96);
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

    public boolean hasFailed() {
        return failed;
    }

    public long getTruncatedBytesOnOpen() {
        return truncatedBytesOnOpen;
    }

    public Path getFile() {
        return file;
    }

    // Idempotent.
    @Override
    public synchronized void close() throws IOException {
        if (channel.isOpen()) {
            OPEN.decrementAndGet();
            try {
                channel.close();
            } finally {
                lock.release();
            }
        }
    }

    // ---------- Exclusive access (D65) ----------

    /*
    The lock is on a separate "<journal>.lock" file that nothing else ever opens. On Linux and macOS Java's file
    locks are POSIX locks, and closing ANY handle a process holds on a file drops all of that process's locks on
    it; read(), the header check and the torn-tail cut open and close handles on the journal itself, so a lock on
    the journal would vanish. For the same reason a second open in this JVM is refused by HELD before it opens the
    lock file at all. The lock file stays after close (deleting it would race with the next opener); it is empty.
    */
    private record Lock(Path key, FileChannel channel) {

        static Lock acquire(Path journal) throws IOException {
            Path lockFile = journal.resolveSibling(journal.getFileName() + ".lock");
            try {
                Files.createFile(lockFile);     // create-new: never opens, so never closes, an existing lock file
            } catch (FileAlreadyExistsException normalAfterTheFirstRun) {
                // reuse it
            }
            Path key = lockFile.toRealPath();   // one key however the journal path is spelled
            if (!HELD.add(key)) {
                throw alreadyOpen(journal);
            }
            FileChannel channel = null;
            try {
                channel = FileChannel.open(lockFile, WRITE);
                if (channel.tryLock() == null) {
                    throw alreadyOpen(journal);
                }
                return new Lock(key, channel);
            } catch (IOException | RuntimeException e) {
                try {
                    if (channel != null) {
                        channel.close();
                    }
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                HELD.remove(key);
                throw e;
            }
        }

        // Closing the channel releases the OS lock.
        void release() throws IOException {
            try {
                channel.close();
            } finally {
                HELD.remove(key);
            }
        }

        private static IllegalStateException alreadyOpen(Path journal) {
            return new IllegalStateException("Journal " + journal + " is already open (a running exchange, or a --quarantine run)");
        }
    }

    // ---------- Recovery ----------

    // Refuses, before anything is cut or written, a file that is not a journal written under these rules. A file
    // with no complete record may only be the start of a header (a crash while the journal was being created).
    private static void checkHeader(Path file, int rulesVersion) throws IOException {
        FirstRecord first = readFirstRecord(file);
        if (first.complete()) {
            int written = JournalCodec.decodeHeader(first.text());
            if (written != rulesVersion) {
                throw new IllegalStateException("Journal " + file + " was written under rules version " + written
                        + "; this build uses " + rulesVersion + ". Start a fresh journal from a snapshot.");
            }
        } else if (!JournalCodec.encodeHeader(rulesVersion).startsWith(first.text())) {
            throw new IllegalStateException("Journal " + file + " does not start with a journal header: \"" + first.text() + "\"");
        }
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
        return new FirstRecord(record.toString(UTF_8), complete);
    }
}
