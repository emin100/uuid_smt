package io.debezium.uuid;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Converts selected {@link Schema.Type#BYTES} fields (MySQL {@code BINARY(16)} / {@code VARBINARY(16)})
 * into canonical UUID strings for sinks such as PostgreSQL {@code UUID}.
 *
 * <p>Recurses into nested structs (e.g. Debezium envelope {@code before} / {@code after}).
 *
 * <p>Example (sink connector):
 * <pre>
 * transforms=bin2uuid
 * transforms.bin2uuid.type=io.debezium.uuid.BinaryToUuid$Value
 * transforms.bin2uuid.fields=id,tenant_id
 * </pre>
 */
public abstract class BinaryToUuid<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELDS_CONFIG = UuidSmtConfig.FIELDS;
    public static final String BYTE_ORDER_CONFIG = UuidSmtConfig.BYTE_ORDER;
    public static final String RFC_4122 = UuidSmtConfig.RFC_4122;
    /** Matches MySQL {@code UUID_TO_BIN(uuid, 1)} layout (index-friendly swap). */
    public static final String MYSQL_UUID_TO_BIN_SWAP = UuidSmtConfig.MYSQL_UUID_TO_BIN_SWAP;

    private static final ConfigDef CONFIG_DEF =
            UuidSmtConfig.configDef(
                    "Comma-separated field names to convert from BYTES to UUID string when length is 16.");

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
        copySchemaProps(schema, builder);
        boolean changed = false;
        for (Field f : schema.fields()) {
            if (fieldNames.contains(f.name()) && f.schema().type() == Schema.Type.BYTES) {
                Schema stringField =
                        f.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
                builder.field(f.name(), stringField);
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

    static void copySchemaProps(Schema source, SchemaBuilder target) {
        if (source.name() != null) {
            target.name(source.name());
        }
        if (source.version() != null) {
            target.version(source.version());
        }
        if (source.doc() != null) {
            target.doc(source.doc());
        }
        if (source.parameters() != null) {
            target.parameters(source.parameters());
        }
        if (source.isOptional()) {
            target.optional();
        }
    }

    static Struct copyStruct(Struct source, Schema newSchema, Set<String> fieldNames, UuidByteOrder mode) {
        Struct dest = new Struct(newSchema);
        for (Field f : newSchema.fields()) {
            if (fieldNames.contains(f.name()) && f.schema().type() == Schema.Type.STRING) {
                Field oldField = source.schema().field(f.name());
                if (oldField != null && oldField.schema().type() == Schema.Type.BYTES) {
                    byte[] raw = source.getBytes(f.name());
                    dest.put(f.name(), bytesToUuidString(raw, mode));
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

    static String bytesToUuidString(byte[] bytes, UuidByteOrder mode) {
        return UuidBytes.bytesToUuidString(bytes, mode);
    }

    static void mysqlOrderedBinaryToRfcInPlace(byte[] b) {
        UuidBytes.mysqlOrderedBinaryToRfcInPlace(b);
    }

    static void rfcBinaryToMysqlOrderedInPlace(byte[] b) {
        UuidBytes.rfcBinaryToMysqlOrderedInPlace(b);
    }

    public static class Key<R extends ConnectRecord<R>> extends BinaryToUuid<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends BinaryToUuid<R> {
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
