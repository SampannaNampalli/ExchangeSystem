package exchangeserver.journal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.WRITE;

/*

One process, one owner per journal (D65). Held by an open CommandJournal, or by a tool (compaction) that must keep
the journal to itself while it replaces the file.

The lock is on a separate "<journal>.lock" file that nothing else ever opens. On Linux and macOS Java's file locks
are POSIX locks, and closing ANY handle a process holds on a file drops all of that process's locks on it; the
journal code opens and closes handles on the journal itself all the time, so a lock there would vanish. For the
same reason a second acquire in this JVM is refused by HELD before it opens the lock file at all. The lock file
stays after release (deleting it would race with the next acquirer); it is empty.

 */
public final class JournalLock implements AutoCloseable {

    private static final Set<Path> HELD = ConcurrentHashMap.newKeySet();

    private final Path key;
    private final FileChannel channel;

    private JournalLock(Path key, FileChannel channel) {
        this.key = key;
        this.channel = channel;
    }

    public static JournalLock acquire(Path journal) throws IOException {
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
            return new JournalLock(key, channel);
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

    // Idempotent. Closing the channel releases the OS lock.
    @Override
    public synchronized void close() throws IOException {
        if (!channel.isOpen()) {
            return;
        }
        try {
            channel.close();
        } finally {
            HELD.remove(key);
        }
    }

    private static IllegalStateException alreadyOpen(Path journal) {
        return new IllegalStateException("Journal " + journal + " is already open (a running exchange, or a --quarantine or --compact run)");
    }
}
