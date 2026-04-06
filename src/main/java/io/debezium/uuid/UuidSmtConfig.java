package io.debezium.uuid;

import org.apache.kafka.common.config.ConfigDef;

/** Shared Kafka Connect config keys and {@link ConfigDef} for UUID SMTs. */
public final class UuidSmtConfig {

    public static final String FIELDS = "fields";
    public static final String BYTE_ORDER = "byte.order";
    public static final String RFC_4122 = "RFC_4122";
    public static final String MYSQL_UUID_TO_BIN_SWAP = "MYSQL_UUID_TO_BIN_SWAP";

    private UuidSmtConfig() {}

    static ConfigDef configDef(String fieldsDoc) {
        return new ConfigDef()
                .define(
                        FIELDS,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        fieldsDoc)
                .define(
                        BYTE_ORDER,
                        ConfigDef.Type.STRING,
                        RFC_4122,
                        ConfigDef.ValidString.in(RFC_4122, MYSQL_UUID_TO_BIN_SWAP),
                        ConfigDef.Importance.MEDIUM,
                        "Binary layout: RFC_4122 (default) or MYSQL_UUID_TO_BIN_SWAP for UUID_TO_BIN(uuid,1).");
    }
}
