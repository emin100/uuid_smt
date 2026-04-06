package io.debezium.uuid;

/** Wire layout for 16-byte UUID binary encodings. */
public enum UuidByteOrder {
    RFC_4122,
    /** MySQL {@code UUID_TO_BIN(uuid, 1)} / Percona ordered layout. */
    MYSQL_UUID_TO_BIN_SWAP
}
