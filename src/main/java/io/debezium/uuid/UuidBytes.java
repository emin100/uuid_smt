package io.debezium.uuid;

import java.util.Arrays;
import java.util.UUID;

/** RFC 4122 wire encoding for 128-bit UUIDs (big-endian msb/lsb). */
final class UuidBytes {

    private static final ThreadLocal<byte[]> SWAP_SCRATCH = ThreadLocal.withInitial(() -> new byte[255]);

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

    static String bytesToUuidString(byte[] bytes, UuidByteOrder mode) {
        if (bytes == null) {
            return null;
        }
        if (bytes.length != 16) {
            // BINARY(255) gibi alanlar: 36 karakterlik ASCII metin UUID + 0x00 dolgu
            return textBytesToUuidString(bytes);
        }
        if (mode == UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP) {
            byte[] work = Arrays.copyOf(bytes, 16);
            mysqlOrderedBinaryToRfcInPlace(work);
            return rfcBytesToUuidString(work);
        }
        return rfcBytesToUuidString(bytes);
    }

    /**
     * 0x00 dolgulu metin UUID ({@code 8-4-4-4-12}, 36 ASCII karakter) -> kucuk harfli UUID string.
     * Gecersiz ise sessizce gecmez, DataException firlatir.
     */
    static String textBytesToUuidString(byte[] bytes) {
        int end = 0;
        while (end < bytes.length && bytes[end] != 0) {
            end++;
        }
        String text = new String(bytes, 0, end, java.nio.charset.StandardCharsets.US_ASCII).toLowerCase(java.util.Locale.ROOT);
        if (text.length() != 36
                || text.charAt(8) != '-'
                || text.charAt(13) != '-'
                || text.charAt(18) != '-'
                || text.charAt(23) != '-') {
            throw new org.apache.kafka.connect.errors.DataException(
                    "Expected 16 raw bytes or a 36-char text UUID, got length " + bytes.length + " value: " + text);
        }
        try {
            return UUID.fromString(text).toString();
        } catch (IllegalArgumentException e) {
            throw new org.apache.kafka.connect.errors.DataException("Invalid UUID text: " + text, e);
        }
    }
}
