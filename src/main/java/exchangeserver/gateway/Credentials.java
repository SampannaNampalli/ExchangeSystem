package exchangeserver.gateway;

import exchangeserver.domain.Rules;
import exchangeserver.journal.Durable;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/*

Who may log in as which participant (D74). The users file holds one line per participant:

    <participant>:pbkdf2-sha256:<iterations>:<salt base64>:<hash base64>

The secret itself is never stored: PBKDF2-HMAC-SHA256 with a random 16-byte salt and many iterations makes a
stolen file expensive to brute-force. Verification compares in constant time, and an unknown participant costs the
same as a known one, so timing does not reveal which names exist. Secrets are never logged.

Immutable once loaded; safe to share between session threads.

 */
public final class Credentials {

    public static final int DEFAULT_ITERATIONS = 210_000;
    // Printable ASCII without spaces (the protocol separates fields with spaces), 8 to 64 characters.
    public static final Pattern SECRET = Pattern.compile("[\\x21-\\x7E]{8,64}");

    private static final String SCHEME = "pbkdf2-sha256";
    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 256;

    private record Entry(int iterations, byte[] salt, byte[] hash) {
    }

    private final Map<String, Entry> entries;
    private final Entry dummy;

    private Credentials(Map<String, Entry> entries) {
        this.entries = Map.copyOf(entries);
        this.dummy = entries.values().stream().findFirst()
                .orElseGet(() -> new Entry(DEFAULT_ITERATIONS, new byte[SALT_BYTES], new byte[HASH_BITS / 8]));
    }

    public static Credentials load(Path file) throws IOException {
        if (!Files.exists(file)) {
            throw new IllegalStateException("No users file " + file.toAbsolutePath()
                    + ": create it with --add-user <participant> --users " + file);
        }
        return new Credentials(parse(file));
    }

    // For tests and tools: participant -> secret, hashed here.
    public static Credentials of(Map<String, String> secrets, int iterations) {
        Map<String, Entry> entries = new TreeMap<>();
        secrets.forEach((p, s) -> entries.put(validParticipant(p), hash(validSecret(s), iterations)));
        return new Credentials(entries);
    }

    // Adds or replaces one participant's secret, rewriting the file atomically.
    public static void addUser(Path file, String participant, String secret, int iterations) throws IOException {
        Map<String, Entry> entries = Files.exists(file) ? parse(file) : new TreeMap<>();
        entries.put(validParticipant(participant), hash(validSecret(secret), iterations));
        StringBuilder sb = new StringBuilder();
        entries.forEach((p, e) -> sb.append(p).append(':').append(SCHEME).append(':').append(e.iterations()).append(':')
                .append(Base64.getEncoder().encodeToString(e.salt())).append(':')
                .append(Base64.getEncoder().encodeToString(e.hash())).append('\n'));
        Durable.writeAtomically(file, sb.toString());
    }

    public boolean verify(String participant, String secret) {
        Entry entry = participant == null ? null : entries.get(participant);
        Entry against = entry == null ? dummy : entry;      // same work either way
        byte[] candidate = pbkdf2(secret == null ? "" : secret, against.salt(), against.iterations());
        return MessageDigest.isEqual(candidate, against.hash()) && entry != null;
    }

    public int size() {
        return entries.size();
    }

    private static Map<String, Entry> parse(Path file) throws IOException {
        Map<String, Entry> entries = new TreeMap<>();
        int lineNumber = 0;
        for (String line : Files.readAllLines(file, UTF_8)) {
            lineNumber++;
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split(":", -1);
            try {
                if (f.length != 5 || !f[1].equals(SCHEME)) {
                    throw new IllegalArgumentException("expected participant:" + SCHEME + ":iterations:salt:hash");
                }
                Entry e = new Entry(Integer.parseInt(f[2]), Base64.getDecoder().decode(f[3]), Base64.getDecoder().decode(f[4]));
                if (e.iterations() < 1 || e.hash().length != HASH_BITS / 8) {
                    throw new IllegalArgumentException("bad iterations or hash length");
                }
                if (entries.put(validParticipant(f[0]), e) != null) {
                    throw new IllegalArgumentException("participant " + f[0] + " appears twice");
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Users file " + file + " line " + lineNumber + ": " + e.getMessage(), e);
            }
        }
        return entries;
    }

    private static Entry hash(String secret, int iterations) {
        byte[] salt = new byte[SALT_BYTES];
        new SecureRandom().nextBytes(salt);
        return new Entry(iterations, salt, pbkdf2(secret, salt, iterations));
    }

    private static byte[] pbkdf2(String secret, byte[] salt, int iterations) {
        PBEKeySpec spec = new PBEKeySpec(secret.toCharArray(), salt, iterations, HASH_BITS);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 unavailable", e);   // part of every JDK 17
        } finally {
            spec.clearPassword();
        }
    }

    private static String validParticipant(String participant) {
        if (participant == null || !Rules.PARTICIPANT.matcher(participant).matches()) {
            throw new IllegalArgumentException("Invalid participant: " + participant);
        }
        return participant;
    }

    private static String validSecret(String secret) {
        if (secret == null || !SECRET.matcher(secret).matches()) {
            throw new IllegalArgumentException("A secret is 8 to 64 printable ASCII characters without spaces");
        }
        return secret;
    }
}
