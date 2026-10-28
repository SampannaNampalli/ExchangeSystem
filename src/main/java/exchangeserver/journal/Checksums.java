package exchangeserver.journal;

import java.util.zip.CRC32C;

import static java.nio.charset.StandardCharsets.UTF_8;

// The per-record checksum of the journal and the snapshot (D69): CRC-32C of the record's UTF-8 text, 8 hex digits.
public final class Checksums {

    private Checksums() {
    }

    public static String crcHex(String text) {
        CRC32C crc = new CRC32C();
        crc.update(text.getBytes(UTF_8));
        return String.format("%08x", crc.getValue());
    }
}
