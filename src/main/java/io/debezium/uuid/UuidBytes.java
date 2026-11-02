package io.debezium.uuid;

import java.util.Arrays;
import java.util.UUID;

/** RFC 4122 wire encoding for 128-bit UUIDs (big-endian msb/lsb). */
final class UuidBytes {

    private static final ThreadLocal<byte[]> SWAP_SCRATCH = ThreadLocal.withInitial(() -> new byte[16]);

    private UuidBytes() {}

    static String rfcBytesToUuidString(byte[] rfc16) {
        return new UUID(readLongBigEndian(rfc16, 0), readLongBigEndian(rfc16, 8)).toString();
    }

    static byte[] uuidToRfcBytes(UUID u) {
        byte[] b = new byte[16];
        writeLongBigEndian(b, 0, u.getMostSignificantBits());
        writeLongBigEndian(b, 8, u.getLeastSignificantBits());
        return b;
    }

    private static long readLongBigEndian(byte[] b, int offset) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[offset + i] & 0xffL);
        }
        return v;
    }

    static void writeLongBigEndian(byte[] b, int offset, long v) {
        for (int i = 0; i < 8; i++) {
            b[offset + i] = (byte) (v >>> (56 - 8 * i));
        }
    }

    /**
     * MySQL {@code UUID_TO_BIN(uuid, 1)} layout → RFC wire order; uses a thread-local scratch buffer instead
     * of allocating.
     */
    static void mysqlOrderedBinaryToRfcInPlace(byte[] b) {
        byte[] t = SWAP_SCRATCH.get();
        System.arraycopy(b, 0, t, 0, 16);
        System.arraycopy(t, 4, b, 0, 4);
        System.arraycopy(t, 2, b, 4, 2);
        System.arraycopy(t, 0, b, 6, 2);
        System.arraycopy(t, 8, b, 8, 8);
    }

    /** RFC wire bytes → MySQL {@code UUID_TO_BIN(uuid, 1)} layout. */
    static void rfcBinaryToMysqlOrderedInPlace(byte[] b) {
        byte[] t = SWAP_SCRATCH.get();
        System.arraycopy(b, 0, t, 0, 16);
        System.arraycopy(t, 6, b, 0, 2);
        System.arraycopy(t, 4, b, 2, 2);
        System.arraycopy(t, 0, b, 4, 4);
        System.arraycopy(t, 8, b, 8, 8);
    }

    /**
     * Converts a BYTES value to a canonical UUID string.
     *
     * <ul>
     *   <li>exactly 16 bytes: binary UUID, decoded according to {@code mode};
     *   <li>any other length (e.g. {@code BINARY(255)}): textual UUID (36 ASCII chars) optionally followed by
     *       {@code 0x00} padding, see {@link #paddedTextToUuidString(byte[])}.
     * </ul>
     *
     * A 36-char text can never fit in 16 bytes, so the two forms are unambiguous.
     */
    static String bytesToUuidString(byte[] bytes, UuidByteOrder mode) {
        if (bytes == null) {
            return null;
        }
        if (bytes.length != 16) {
            return paddedTextToUuidString(bytes);
        }
        if (mode == UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP) {
            byte[] work = Arrays.copyOf(bytes, 16);
            mysqlOrderedBinaryToRfcInPlace(work);
            return rfcBytesToUuidString(work);
        }
        return rfcBytesToUuidString(bytes);
    }

    /**
     * Text UUID stored in a fixed-width binary column (MySQL {@code BINARY(n)} pads with {@code 0x00}).
     * Cuts at the first {@code 0x00}, validates the 8-4-4-4-12 form strictly and returns it lower-cased.
     */
    static String paddedTextToUuidString(byte[] bytes) {
        int end = bytes.length;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0) {
                end = i;
                break;
            }
        }
        String text = new String(bytes, 0, end, java.nio.charset.StandardCharsets.US_ASCII);
        if (!isCanonicalUuidText(text)) {
            throw new org.apache.kafka.connect.errors.DataException(
                    "Expected 16-byte binary UUID or 36-char text UUID (optionally 0x00-padded), got length "
                            + bytes.length + " with content up to first 0x00: '" + text + "'");
        }
        return text.toLowerCase(java.util.Locale.ROOT);
    }

    private static boolean isCanonicalUuidText(String t) {
        if (t.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char c = t.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (c != '-') {
                    return false;
                }
            } else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }
}
