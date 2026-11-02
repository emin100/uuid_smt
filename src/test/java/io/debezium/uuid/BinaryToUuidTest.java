package io.debezium.uuid;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class BinaryToUuidTest {

    @Test
    void rfcLayout_roundTrip() {
        UUID expected = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        byte[] bytes = uuidToRfcBytes(expected);
        assertEquals(expected.toString(), BinaryToUuid.bytesToUuidString(bytes, UuidByteOrder.RFC_4122));
    }

    @Test
    void mysqlSwap_layout_fromPerconaBlogExample() {
        // Ordered binary: UNHEX('11d8eebc58e0a7d796690800200c9a66') — see Percona ordered_uuid layout.
        byte[] ordered = HexFormat.of().parseHex("41d4e29b550e8400a716446655440000");
        assertEquals(
                "550e8400-e29b-41d4-a716-446655440000",
                BinaryToUuid.bytesToUuidString(ordered, UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP));
    }

    @Test
    void buildSchema_noMatch_returnsSameInstance() {
        Schema s = SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).build();
        assertSame(s, BinaryToUuid.buildSchema(s, Set.of("id")));
    }

    @Test
    void valueTransform_debeziumAfter_nested() {
        Schema rowSchema = SchemaBuilder.struct()
                .name("dbserver.inventory.products.Value")
                .field("id", Schema.BYTES_SCHEMA)
                .optional()
                .build();
        Schema envelopeSchema =
                SchemaBuilder.struct().field("before", rowSchema).field("after", rowSchema).build();

        byte[] id = uuidToRfcBytes(UUID.fromString("11111111-2222-3333-4444-555555555555"));
        Struct after = new Struct(rowSchema).put("id", id);
        Struct value = new Struct(envelopeSchema).put("before", null).put("after", after);

        BinaryToUuid.Value<SourceRecord> smt = new BinaryToUuid.Value<>();
        smt.configure(Map.of(BinaryToUuid.FIELDS_CONFIG, "id"));

        SourceRecord in = new SourceRecord(null, null, "t", null, null, null, envelopeSchema, value);
        SourceRecord out = smt.apply(in);

        Struct outAfter = out.valueSchema().field("after").schema().type() == Schema.Type.STRUCT
                ? ((Struct) out.value()).getStruct("after")
                : null;
        assertEquals("11111111-2222-3333-4444-555555555555", outAfter.getString("id"));
    }

    @Test
    void paddedText255_isConvertedToUuid() {
        byte[] padded = paddedText("3fa85f64-5717-4562-b3fc-2c963f66afa6", 255);
        assertEquals(
                "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                BinaryToUuid.bytesToUuidString(padded, UuidByteOrder.RFC_4122));
    }

    @Test
    void paddedText_upperCase_isLowerCased_andUnpadded36Works() {
        String u = "3FA85F64-5717-4562-B3FC-2C963F66AFA6";
        assertEquals(u.toLowerCase(), BinaryToUuid.bytesToUuidString(paddedText(u, 36), UuidByteOrder.RFC_4122));
    }

    @Test
    void paddedText_byteOrderIsIgnored() {
        byte[] padded = paddedText("3fa85f64-5717-4562-b3fc-2c963f66afa6", 255);
        assertEquals(
                "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                BinaryToUuid.bytesToUuidString(padded, UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP));
    }

    @Test
    void invalidText_throws() {
        assertThrows(
                DataException.class,
                () -> BinaryToUuid.bytesToUuidString(paddedText("not-a-uuid", 255), UuidByteOrder.RFC_4122));
        assertThrows(
                DataException.class,
                () -> BinaryToUuid.bytesToUuidString(new byte[255], UuidByteOrder.RFC_4122));
        assertThrows(
                DataException.class,
                () -> BinaryToUuid.bytesToUuidString(
                        paddedText("3fa85f64-5717-4562-b3fc-2c963f66afaZ", 255), UuidByteOrder.RFC_4122));
    }

    @Test
    void nullBytes_returnNull() {
        assertNull(BinaryToUuid.bytesToUuidString(null, UuidByteOrder.RFC_4122));
    }

    @Test
    void keyTransform_paddedText() {
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.BYTES_SCHEMA).build();
        Struct key = new Struct(keySchema).put("id", paddedText("3fa85f64-5717-4562-b3fc-2c963f66afa6", 255));

        BinaryToUuid.Key<SourceRecord> smt = new BinaryToUuid.Key<>();
        smt.configure(Map.of(BinaryToUuid.FIELDS_CONFIG, "id"));

        SourceRecord out = smt.apply(new SourceRecord(null, null, "t", null, keySchema, key, null, null));
        assertEquals("3fa85f64-5717-4562-b3fc-2c963f66afa6", ((Struct) out.key()).getString("id"));
    }

    private static byte[] paddedText(String uuid, int width) {
        byte[] out = new byte[width];
        byte[] text = uuid.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        System.arraycopy(text, 0, out, 0, text.length);
        return out;
    }

    private static byte[] uuidToRfcBytes(UUID u) {
        ByteBuffer bb = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
        bb.putLong(u.getMostSignificantBits());
        bb.putLong(u.getLeastSignificantBits());
        return bb.array();
    }
}
