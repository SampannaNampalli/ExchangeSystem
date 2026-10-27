package exchangeserver.gateway;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CredentialsTest {

    @TempDir
    Path dir;

    @Test
    void verifiesOnlyTheRightSecretForTheRightParticipant() {
        Credentials c = Credentials.of(Map.of("alice", "alice-secret", "bob", "bob-secret!"), 1_000);
        assertTrue(c.verify("alice", "alice-secret"));
        assertFalse(c.verify("alice", "bob-secret!"));
        assertFalse(c.verify("carol", "alice-secret"));
        assertFalse(c.verify(null, "alice-secret"));
        assertFalse(c.verify("alice", null));
    }

    @Test
    void theFileStoresSaltedHashesAndAnUpdateReplacesTheEntry() throws IOException {
        Path users = dir.resolve("exchange.users");
        Credentials.addUser(users, "alice", "first-secret", 1_000);
        Credentials.addUser(users, "alice", "second-secret", 1_000);
        Credentials.addUser(users, "bob", "first-secret", 1_000);
        Credentials loaded = Credentials.load(users);
        assertEquals(2, loaded.size());
        assertTrue(loaded.verify("alice", "second-secret"));
        assertFalse(loaded.verify("alice", "first-secret"));
        String text = Files.readString(users, UTF_8);
        assertFalse(text.contains("secret"), "no secret in the file");
        String[] lines = text.split("\n");
        assertFalse(lines[0].split(":")[4].equals(lines[1].split(":")[4]), "salted: the same secret hashes differently");
    }

    @Test
    void weakOrMalformedInputIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> Credentials.of(Map.of("alice", "short"), 1_000));
        assertThrows(IllegalArgumentException.class, () -> Credentials.of(Map.of("alice", "has a space"), 1_000));
        assertThrows(IllegalArgumentException.class, () -> Credentials.of(Map.of("bad name", "long-enough"), 1_000));
    }

    @Test
    void aMissingFileTellsTheOperatorWhatToDo() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Credentials.load(dir.resolve("none")));
        assertTrue(e.getMessage().contains("--add-user"), e.getMessage());
    }

    @Test
    void aCorruptFileNamesTheLine() throws IOException {
        Path users = dir.resolve("exchange.users");
        Files.writeString(users, "# comment\nalice:md5:1:x:y\n", UTF_8);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Credentials.load(users));
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
    }
}
