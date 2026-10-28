package exchangeserver.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

// File operations that survive a crash at any point (D69, D71).
public final class Durable {

    private Durable() {
    }

    // Replaces `target` with `content` so that after a crash it holds either the old or the new content, never a
    // mix: write a temporary file, fsync it, rename it over the target, fsync the directory.
    public static void writeAtomically(Path target, String content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, CREATE, TRUNCATE_EXISTING, WRITE)) {
            ByteBuffer bytes = ByteBuffer.wrap(content.getBytes(UTF_8));
            while (bytes.hasRemaining()) {
                ch.write(bytes);
            }
            ch.force(true);
        }
        Files.move(tmp, target, ATOMIC_MOVE, REPLACE_EXISTING);
        fsyncDirectory(target);
    }

    // Copies `source` to `target` and fsyncs the copy (not atomic: for archives, which nothing reads back).
    public static void copyDurably(Path source, Path target) throws IOException {
        Files.copy(source, target, REPLACE_EXISTING);
        try (FileChannel ch = FileChannel.open(target, WRITE)) {
            ch.force(true);
        }
        fsyncDirectory(target);
    }

    // Makes a file's creation or rename durable: on Linux a new directory entry can be lost in a crash until the
    // directory itself is fsynced. Windows cannot open a directory as a file and journals its metadata itself,
    // so failing to open it is ignored there.
    public static void fsyncDirectory(Path file) throws IOException {
        Path dir = file.toAbsolutePath().getParent();
        FileChannel ch;
        try {
            ch = FileChannel.open(dir, READ);
        } catch (IOException notSupportedHere) {
            return;
        }
        try (ch) {
            ch.force(true);
        } catch (IOException notSupportedHere) {
            // some platforms refuse fsync on a directory handle; nothing more can be done
        }
    }
}
