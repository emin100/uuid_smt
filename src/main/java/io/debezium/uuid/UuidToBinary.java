package io.debezium.uuid;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Converts selected {@link Schema.Type#STRING} fields holding canonical UUID text (e.g. PostgreSQL {@code
 * uuid}) into {@link Schema.Type#BYTES} with length 16 for MySQL {@code BINARY(16)} / {@code
 * VARBINARY(16)} sinks.
 *
 * <p>Recurses into nested structs (e.g. Debezium envelope {@code before} / {@code after}).
 *
 * <p>Example (sink to MySQL):
 * <pre>
 * transforms=uuid2bin
 * transforms.uuid2bin.type=io.debezium.uuid.UuidToBinary$Value
 * transforms.uuid2bin.fields=id,tenant_id
 * </pre>
 */
public abstract class UuidToBinary<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_CONFIG = UuidSmtConfig.FIELDS;
    public static final String BYTE_ORDER_CONFIG = UuidSmtConfig.BYTE_ORDER;
    public static final String RFC_4122 = UuidSmtConfig.RFC_4122;
    /** Write bytes in MySQL {@code UUID_TO_BIN(uuid, 1)} / Percona ordered layout. */
    public static final String MYSQL_UUID_TO_BIN_SWAP = UuidSmtConfig.MYSQL_UUID_TO_BIN_SWAP;

    private static final ConfigDef CONFIG_DEF =
            UuidSmtConfig.configDef(
                    "Comma-separated field names to convert from UUID string to 16-byte binary.");

    protected Set<String> fields;
    protected UuidByteOrder byteOrderMode;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig cfg = new AbstractConfig(CONFIG_DEF, configs);
        fields = new HashSet<>(cfg.getList(UuidSmtConfig.FIELDS));
        if (fields.isEmpty()) {
            throw new org.apache.kafka.common.config.ConfigException(
                    UuidSmtConfig.FIELDS + " must list at least one field name.");
        }
        byteOrderMode =
                UuidSmtConfig.MYSQL_UUID_TO_BIN_SWAP.equalsIgnoreCase(cfg.getString(UuidSmtConfig.BYTE_ORDER))
                        ? UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP
                        : UuidByteOrder.RFC_4122;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    @Override
    public R apply(R record) {
        Object v = operatingValue(record);
        if (v == null || !(v instanceof Struct)) {
            return record;
        }
        Struct input = (Struct) v;
        Schema newSchema = buildSchema(input.schema(), fields);
        if (newSchema == null || newSchema == input.schema()) {
            return record;
        }
        Struct output = copyStruct(input, newSchema, fields, byteOrderMode);
        return newRecord(record, newSchema, output);
    }

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema schema, Struct updatedValue);

    static Schema buildSchema(Schema schema, Set<String> fieldNames) {
        if (schema == null) {
            return null;
        }
        if (schema.type() != Schema.Type.STRUCT) {
            return schema;
        }
        SchemaBuilder builder = SchemaBuilder.struct();
        BinaryToUuid.copySchemaProps(schema, builder);
        boolean changed = false;
        for (Field f : schema.fields()) {
            if (fieldNames.contains(f.name()) && f.schema().type() == Schema.Type.STRING) {
                Schema bytesField = f.schema().isOptional() ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
                builder.field(f.name(), bytesField);
                changed = true;
            } else if (f.schema().type() == Schema.Type.STRUCT) {
                Schema nested = buildSchema(f.schema(), fieldNames);
                Schema use = nested != null ? nested : f.schema();
                if (!use.equals(f.schema())) {
                    changed = true;
                }
                builder.field(f.name(), use);
            } else {
                builder.field(f.name(), f.schema());
            }
        }
        return changed ? builder.build() : schema;
    }

    static Struct copyStruct(Struct source, Schema newSchema, Set<String> fieldNames, UuidByteOrder mode) {
        Struct dest = new Struct(newSchema);
        for (Field f : newSchema.fields()) {
            if (fieldNames.contains(f.name()) && f.schema().type() == Schema.Type.BYTES) {
                Field oldField = source.schema().field(f.name());
                if (oldField != null && oldField.schema().type() == Schema.Type.STRING) {
                    String s = source.getString(f.name());
                    dest.put(f.name(), uuidStringToBytes(s, mode));
                }
            } else if (f.schema().type() == Schema.Type.STRUCT) {
                Struct nested = source.getStruct(f.name());
                dest.put(f.name(), nested == null ? null : copyStruct(nested, f.schema(), fieldNames, mode));
            } else {
                dest.put(f.name(), source.get(f));
            }
        }
        return dest;
    }

    static byte[] uuidStringToBytes(String s, UuidByteOrder mode) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        if (t.isEmpty()) {
            throw new org.apache.kafka.connect.errors.DataException("UUID string is empty");
        }
        UUID uuid = UUID.fromString(t);
        byte[] rfc = UuidBytes.uuidToRfcBytes(uuid);
        if (mode == UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP) {
            UuidBytes.rfcBinaryToMysqlOrderedInPlace(rfc);
        }
        return rfc;
    }

    public static class Key<R extends ConnectRecord<R>> extends UuidToBinary<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema schema, Struct updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    schema,
                    updatedValue,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp(),
                    record.headers());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends UuidToBinary<R> {
        @Override
        public R apply(R record) {
            Schema newValueSchema = record.valueSchema();
            Object newValue = record.value();
            if (record.value() instanceof Struct input) {
                Schema built = buildSchema(input.schema(), this.fields);
                if (built != null && built != input.schema()) {
                    newValueSchema = built;
                    newValue = copyStruct(input, built, this.fields, this.byteOrderMode);
                }
            }

            Schema newKeySchema = record.keySchema();
            Object newKey = record.key();
            if (record.key() instanceof Struct input) {
                Schema built = buildSchema(input.schema(), this.fields);
                if (built != null && built != input.schema()) {
                    newKeySchema = built;
                    newKey = copyStruct(input, built, this.fields, this.byteOrderMode);
                }
            }

            if (newKeySchema == record.keySchema()
                    && newKey == record.key()
                    && newValueSchema == record.valueSchema()
                    && newValue == record.value()) {
                return record;
            }
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    newKeySchema,
                    newKey,
                    newValueSchema,
                    newValue,
                    record.timestamp(),
                    record.headers());
        }
    
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema schema, Struct updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    schema,
                    updatedValue,
                    record.timestamp(),
                    record.headers());
        }
    }

}
