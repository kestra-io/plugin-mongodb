package io.kestra.plugin.mongodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.InsertOneModel;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class LoadExtendedJsonTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldEncodeBsonTypesWhenFieldsAreExtendedJsonWrappers() throws Exception {
        var encoded = encode(
            Map.of(
                "createdAt", Map.of("$date", "2024-01-01T00:00:00Z"),
                "amount", Map.of("$numberDecimal", "12.34"),
                "externalId", Map.of("$oid", "60930c39a982931c20ef6cd6"),
                "count", Map.of("$numberLong", "123"),
                "nested", Map.of("name", "john", "updatedAt", Map.of("$date", Map.of("$numberLong", "1704067200000"))),
                "ids", List.of(Map.of("$oid", "60930c39a982931c20ef6cd6"), Map.of("$numberDecimal", "1.5"), "plain")
            )
        );

        assertThat(encoded.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.getDateTime("createdAt").getValue(), is(1704067200000L));
        assertThat(encoded.get("amount").getBsonType(), is(BsonType.DECIMAL128));
        assertThat(encoded.getDecimal128("amount").getValue().bigDecimalValue(), is(new BigDecimal("12.34")));
        assertThat(encoded.get("externalId").getBsonType(), is(BsonType.OBJECT_ID));
        assertThat(encoded.getObjectId("externalId").getValue().toHexString(), is("60930c39a982931c20ef6cd6"));
        assertThat(encoded.get("count").getBsonType(), is(BsonType.INT64));

        var nested = encoded.getDocument("nested");
        assertThat(nested.get("name").getBsonType(), is(BsonType.STRING));
        assertThat(nested.get("updatedAt").getBsonType(), is(BsonType.DATE_TIME));

        var ids = encoded.getArray("ids");
        assertThat(ids.get(0).getBsonType(), is(BsonType.OBJECT_ID));
        assertThat(ids.get(1).getBsonType(), is(BsonType.DECIMAL128));
        assertThat(ids.get(2).getBsonType(), is(BsonType.STRING));
    }

    @Test
    void shouldKeepOrdinaryAndNativeValuesWhenNotExtendedJson() throws Exception {
        var encoded = encode(
            Map.of(
                "address", Map.of("city", "Paris", "createdAt", "2024-01-01"),
                "tags", List.of("a", Map.of("k", 1)),
                "empty", Map.of(),
                "unknownOperator", Map.of("$foo", Map.of("$date", "2024-01-01T00:00:00Z")),
                "dollarNotFirst", ImmutableMap.of("city", "Paris", "$date", "2024-01-01T00:00:00Z"),
                "price", new BigDecimal("12.34"),
                "payload", new byte[] { 1, 2, 3 }
            )
        );

        assertThat(encoded.get("address").getBsonType(), is(BsonType.DOCUMENT));
        assertThat(encoded.getDocument("address").get("createdAt").getBsonType(), is(BsonType.STRING));
        assertThat(encoded.get("tags").getBsonType(), is(BsonType.ARRAY));
        assertThat(encoded.getArray("tags").get(1).getBsonType(), is(BsonType.DOCUMENT));
        assertThat(encoded.getDocument("empty").isEmpty(), is(true));
        assertThat(encoded.getDocument("unknownOperator").get("$foo").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.getDocument("dollarNotFirst").get("$date").getBsonType(), is(BsonType.STRING));
        assertThat(encoded.get("price").getBsonType(), is(BsonType.DECIMAL128));
        assertThat(encoded.get("payload").getBsonType(), is(BsonType.BINARY));
    }

    @Test
    void shouldEncodeOtherExtendedJsonWrappers() throws Exception {
        var encoded = encode(
            Map.of(
                "payload", Map.of("$binary", ImmutableMap.of("base64", "AQID", "subType", "00")),
                "small", Map.of("$numberInt", "7"),
                "ratio", Map.of("$numberDouble", "1.5"),
                "ts", Map.of("$timestamp", ImmutableMap.of("t", 1700000000, "i", 1)),
                "pattern", Map.of("$regularExpression", ImmutableMap.of("pattern", "^a", "options", "i")),
                "legacyPattern", ImmutableMap.of("$regex", "^a", "$options", "i"),
                "uuid", Map.of("$uuid", "123e4567-e89b-12d3-a456-426614174000")
            )
        );

        assertThat(encoded.get("payload").getBsonType(), is(BsonType.BINARY));
        assertThat(encoded.getBinary("payload").getData(), is(new byte[] { 1, 2, 3 }));
        assertThat(encoded.getInt32("small").getValue(), is(7));
        assertThat(encoded.getDouble("ratio").getValue(), is(1.5));
        assertThat(encoded.getTimestamp("ts").getTime(), is(1700000000));
        assertThat(encoded.getTimestamp("ts").getInc(), is(1));
        assertThat(encoded.getRegularExpression("pattern").getPattern(), is("^a"));
        assertThat(encoded.getRegularExpression("pattern").getOptions(), is("i"));
        assertThat(encoded.get("legacyPattern").getBsonType(), is(BsonType.REGULAR_EXPRESSION));
        assertThat(encoded.getBinary("uuid").asUuid(), is(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")));
    }

    @Test
    void shouldKeepDbRefAsDocument() throws Exception {
        var encoded = encode(
            Map.of("owner", ImmutableMap.of("$ref", "users", "$id", Map.of("$oid", "60930c39a982931c20ef6cd6")))
        );

        var owner = encoded.getDocument("owner");
        assertThat(owner.getString("$ref").getValue(), is("users"));
        assertThat(owner.getObjectId("$id").getValue().toHexString(), is("60930c39a982931c20ef6cd6"));
    }

    @Test
    void shouldAcceptOidWrapperAsIdKey() throws Exception {
        var load = Load.builder().idKey(Property.ofValue("id")).build();

        var encoded = encode(load, List.of(Map.of("id", Map.of("$oid", "60930c39a982931c20ef6cd6"), "name", "john"))).getFirst();

        assertThat(encoded.getObjectId("_id").getValue().toHexString(), is("60930c39a982931c20ef6cd6"));
        assertThat(encoded.containsKey("id"), is(false));
        assertThat(encoded.getString("name").getValue(), is("john"));
    }

    @Test
    void shouldFailWithRecordAndFieldWhenExtendedJsonWrapperIsMalformed() {
        var e = assertThrows(IllegalArgumentException.class, () -> encode(Map.of("createdAt", Map.of("$date", "not-a-date"))));

        assertThat(e.getMessage(), containsString("Record 1, field 'createdAt'"));
        assertThat(e.getMessage(), containsString("$date"));
        assertThat(e.getCause(), instanceOf(JsonParseException.class));

        var nested = assertThrows(
            IllegalArgumentException.class,
            () -> encode(
                Load.builder().build(),
                List.of(
                    Map.of("name", "valid"),
                    Map.of("nested", Map.of("ids", List.of(Map.of("$oid", "not-an-object-id"))))
                )
            )
        );

        assertThat(nested.getMessage(), containsString("Record 2, field 'nested.ids[0]'"));
        assertThat(nested.getMessage(), containsString("$oid"));
    }

    private BsonDocument encode(Map<String, Object> record) throws Exception {
        return encode(Load.builder().build(), List.of(record)).getFirst();
    }

    private List<BsonDocument> encode(Load load, List<Map<String, Object>> records) throws Exception {
        var output = new ByteArrayOutputStream();
        for (var record : records) {
            FileSerde.write(output, record);
        }

        return load.source(runContextFactory.of(), new ByteArrayInputStream(output.toByteArray()))
            .map(model -> ((Document) ((InsertOneModel<?>) model).getDocument()).toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry()))
            .collectList()
            .block();
    }
}
