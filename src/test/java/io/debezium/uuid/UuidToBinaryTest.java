package io.debezium.uuid;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class UuidToBinaryTest {

    @Test
    void rfc_roundTrip_withBinaryToUuid() {
        String s = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        byte[] bytes = UuidToBinary.uuidStringToBytes(s, UuidByteOrder.RFC_4122);
        assertEquals(s, BinaryToUuid.bytesToUuidString(bytes, UuidByteOrder.RFC_4122));
    }

    @Test
    void mysqlSwap_roundTrip_withBinaryToUuid() {
        String s = "550e8400-e29b-41d4-a716-446655440000";
        byte[] ordered = UuidToBinary.uuidStringToBytes(s, UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP);
        assertEquals(s, BinaryToUuid.bytesToUuidString(ordered, UuidByteOrder.MYSQL_UUID_TO_BIN_SWAP));
    }

    @Test
    void textWidth255_padsAsciiText_andRoundTrips() {
        String s = "3FA85F64-5717-4562-B3FC-2C963F66AFA6";
        byte[] out = UuidToBinary.uuidStringToBytes(s, UuidByteOrder.RFC_4122, 255);
        assertEquals(255, out.length);
        assertArrayEquals(s.toLowerCase().getBytes(java.nio.charset.StandardCharsets.US_ASCII), Arrays.copyOf(out, 36));
        for (int i = 36; i < 255; i++) {
            assertEquals(0, out[i]);
        }
        assertEquals(s.toLowerCase(), BinaryToUuid.bytesToUuidString(out, UuidByteOrder.RFC_4122));
    }

    @Test
    void textWidth_invalidConfig_throws() {
        UuidToBinary.Value<SourceRecord> smt = new UuidToBinary.Value<>();
        org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.kafka.common.config.ConfigException.class,
                () -> smt.configure(Map.of(UuidToBinary.FIELDS_CONFIG, "id", UuidToBinary.TEXT_WIDTH_CONFIG, "20")));
    }

    @Test
    void valueTransform_textWidth_nested() {
        Schema rowSchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).optional().build();
        Schema envelopeSchema =
                SchemaBuilder.struct().field("before", rowSchema).field("after", rowSchema).build();
        Struct after = new Struct(rowSchema).put("id", "11111111-2222-3333-4444-555555555555");
        Struct value = new Struct(envelopeSchema).put("before", null).put("after", after);

        UuidToBinary.Value<SourceRecord> smt = new UuidToBinary.Value<>();
        smt.configure(Map.of(UuidToBinary.FIELDS_CONFIG, "id", UuidToBinary.TEXT_WIDTH_CONFIG, "255"));
        SourceRecord out = smt.apply(new SourceRecord(null, null, "t", null, null, null, envelopeSchema, value));
        byte[] id = ((Struct) out.value()).getStruct("after").getBytes("id");
        assertEquals(255, id.length);
        assertEquals("11111111-2222-3333-4444-555555555555", BinaryToUuid.bytesToUuidString(id, UuidByteOrder.RFC_4122));
    }

    @Test
    void rfcBinaryToMysql_isInverseOf_mysqlOrderedToRfc() {
        UUID u = UUID.fromString("58e0a7d7-eebc-11d8-9669-0800200c9a66");
        byte[] rfc = UuidToBinary.uuidStringToBytes(u.toString(), UuidByteOrder.RFC_4122);
        byte[] copy = rfc.clone();
        BinaryToUuid.rfcBinaryToMysqlOrderedInPlace(copy);
        BinaryToUuid.mysqlOrderedBinaryToRfcInPlace(copy);
        assertArrayEquals(rfc, copy);
    }

    @Test
    void valueTransform_nestedStringToBytes() {
        Schema rowSchema = SchemaBuilder.struct()
                .name("pg.public.items.Value")
                .field("id", Schema.STRING_SCHEMA)
                .optional()
                .build();
        Schema envelopeSchema =
                SchemaBuilder.struct().field("before", rowSchema).field("after", rowSchema).build();

        Struct after = new Struct(rowSchema).put("id", "11111111-2222-3333-4444-555555555555");
        Struct value = new Struct(envelopeSchema).put("before", null).put("after", after);

        UuidToBinary.Value<SourceRecord> smt = new UuidToBinary.Value<>();
        smt.configure(Map.of(UuidToBinary.FIELDS_CONFIG, "id"));

        SourceRecord in = new SourceRecord(null, null, "t", null, null, null, envelopeSchema, value);
        SourceRecord out = smt.apply(in);

        Struct outAfter = ((Struct) out.value()).getStruct("after");
        assertEquals(16, outAfter.getBytes("id").length);
        assertEquals(
                "11111111-2222-3333-4444-555555555555",
                BinaryToUuid.bytesToUuidString(outAfter.getBytes("id"), UuidByteOrder.RFC_4122));
    }

    @Test
    void buildSchema_noMatch_returnsSameInstance() {
        Schema s = SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).build();
        assertSame(s, UuidToBinary.buildSchema(s, Set.of("id")));
    }
}
