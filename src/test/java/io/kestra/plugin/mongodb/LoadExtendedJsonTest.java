package io.kestra.plugin.mongodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.InsertOneModel;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
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
    void shouldFailWhenExtendedJsonWrapperIsMalformed() {
        assertThrows(JsonParseException.class, () -> encode(Map.of("createdAt", Map.of("$date", "not-a-date"))));
    }

    private BsonDocument encode(Map<String, Object> record) throws Exception {
        var output = new ByteArrayOutputStream();
        FileSerde.write(output, record);

        var model = (InsertOneModel<?>) Load.builder().build()
            .source(runContextFactory.of(), new ByteArrayInputStream(output.toByteArray()))
            .blockFirst();

        return ((Document) model.getDocument()).toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    }
}
