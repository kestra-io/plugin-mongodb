package io.kestra.plugin.mongodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.Decimal128;
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
    // These are the smallest magnitudes FileSerde reads back as BigInteger instead of a primitive integer.
    private static final BigInteger SMALL_POSITIVE_INTEGER = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    private static final BigInteger NEGATIVE_INTEGER = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
    private static final BigInteger THIRTY_FOUR_DIGIT_INTEGER = new BigInteger("1234567890123456789012345678901234");
    private static final BigInteger INTEGER_WITH_REMOVABLE_TRAILING_ZEROS = new BigInteger("12345678901234567890123456789012340000");
    private static final BigInteger INTEGER_REQUIRING_ROUNDING = new BigInteger("12345678901234567890123456789012345");
    private static final BigInteger MAX_ADJUSTED_EXPONENT_INTEGER = BigInteger.TEN.pow(6144);
    private static final BigInteger INTEGER_OUTSIDE_DECIMAL128_RANGE = BigInteger.TEN.pow(6145);
    private static final BigInteger MAX_FINITE_DECIMAL128_INTEGER =
        BigInteger.TEN.pow(34).subtract(BigInteger.ONE).multiply(BigInteger.TEN.pow(6111));
    private static final BigInteger NESTED_BIG_INTEGER = new BigInteger("98765432101234567890");

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

    @Test
    void shouldEncodeOffsetDateTimeAsUtcBsonDate() throws Exception {
        var encoded = encode(Map.of("createdAt", OffsetDateTime.parse("2024-01-01T10:00:00+02:00")));

        assertThat(encoded.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.getDateTime("createdAt").getValue(), is(1704096000000L));
    }

    @Test
    void shouldEncodeNegativeOffsetDateTimeAsUtcBsonDate() throws Exception {
        var encoded = encode(Map.of("createdAt", OffsetDateTime.parse("2024-01-01T10:00:00-05:00")));

        assertThat(encoded.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.getDateTime("createdAt").getValue(), is(1704121200000L));
    }

    @Test
    void shouldEncodeZonedDateTimeAsUtcBsonDate() throws Exception {
        var encoded = encode(Map.of("createdAt", ZonedDateTime.parse("2024-01-01T10:00:00+01:00[Europe/Paris]")));

        assertThat(encoded.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.getDateTime("createdAt").getValue(), is(1704099600000L));
    }

    @Test
    void shouldEncodeEquivalentInstantsAsIdenticalBsonDates() throws Exception {
        var encoded = encode(
            Map.of(
                "offset", OffsetDateTime.parse("2024-01-01T10:00:00+02:00"),
                "zoned", ZonedDateTime.parse("2024-01-01T09:00:00+01:00[Europe/Paris]")
            )
        );

        assertThat(encoded.get("offset").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.get("zoned").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(encoded.getDateTime("offset").getValue(), is(1704096000000L));
        assertThat(encoded.getDateTime("zoned").getValue(), is(1704096000000L));
        assertThat(encoded.getDateTime("offset"), is(encoded.getDateTime("zoned")));
    }

    @Test
    void shouldUseInstalledDriverRulesForBigIntegerDecimal128Representability() {
        assertDecimal128RoundTrips(BigInteger.valueOf(12345));
        assertDecimal128RoundTrips(BigInteger.valueOf(-987654321));
        assertDecimal128RoundTrips(SMALL_POSITIVE_INTEGER);
        assertDecimal128RoundTrips(NEGATIVE_INTEGER);
        assertDecimal128RoundTrips(THIRTY_FOUR_DIGIT_INTEGER);
        assertDecimal128RoundTrips(INTEGER_WITH_REMOVABLE_TRAILING_ZEROS);

        assertThrows(NumberFormatException.class, () -> decimal128(INTEGER_REQUIRING_ROUNDING));
        assertThrows(AssertionError.class, () -> decimal128(INTEGER_OUTSIDE_DECIMAL128_RANGE));
    }

    @Test
    void shouldEncodeSmallPositiveBigIntegerAsDecimal128() throws Exception {
        var encoded = encode(Map.of("amount", SMALL_POSITIVE_INTEGER));

        assertDecimal128(encoded.get("amount"), SMALL_POSITIVE_INTEGER);
    }

    @Test
    void shouldEncodeNegativeBigIntegerAsDecimal128() throws Exception {
        var encoded = encode(Map.of("amount", NEGATIVE_INTEGER));

        assertDecimal128(encoded.get("amount"), NEGATIVE_INTEGER);
    }

    @Test
    void shouldEncodeThirtyFourDigitBigIntegerAsDecimal128() throws Exception {
        var encoded = encode(Map.of("amount", THIRTY_FOUR_DIGIT_INTEGER));

        assertDecimal128(encoded.get("amount"), THIRTY_FOUR_DIGIT_INTEGER);
    }

    @Test
    void shouldEncodeBigIntegerWithRemovableTrailingZerosAsDecimal128() throws Exception {
        var encoded = encode(Map.of("amount", INTEGER_WITH_REMOVABLE_TRAILING_ZEROS));

        assertDecimal128(encoded.get("amount"), INTEGER_WITH_REMOVABLE_TRAILING_ZEROS);
    }

    @Test
    void shouldEncodeBigIntegerAtMaximumAdjustedExponentAsDecimal128() throws Exception {
        var encoded = encode(Map.of("amount", MAX_ADJUSTED_EXPONENT_INTEGER));

        assertDecimal128(encoded.get("amount"), MAX_ADJUSTED_EXPONENT_INTEGER);
    }

    @Test
    void shouldEncodeMaximumFiniteDecimal128IntegerInBothSigns() throws Exception {
        var encoded = encode(
            Map.of(
                "positive", MAX_FINITE_DECIMAL128_INTEGER,
                "negative", MAX_FINITE_DECIMAL128_INTEGER.negate()
            )
        );

        assertDecimal128(encoded.get("positive"), MAX_FINITE_DECIMAL128_INTEGER);
        assertDecimal128(encoded.get("negative"), MAX_FINITE_DECIMAL128_INTEGER.negate());
    }

    @Test
    void shouldRejectBigIntegerRequiringPrecisionLossWithRecordAndField() {
        var e = assertThrows(IllegalArgumentException.class, () -> encode(Map.of("amount", INTEGER_REQUIRING_ROUNDING)));

        assertThat(e.getMessage(), containsString("Record 1, field 'amount'"));
        assertThat(e.getMessage(), containsString("MongoDB's Decimal128 driver rejected the exact representation"));
        assertThat(e.getMessage(), containsString("store it as a string to keep every digit"));
        assertThat(e.getCause(), instanceOf(NumberFormatException.class));
    }

    @Test
    void shouldRejectIntegerBeyondMaximumFiniteDecimal128InBothSigns() {
        BigInteger justBeyondMaximum = MAX_FINITE_DECIMAL128_INTEGER.add(BigInteger.ONE);

        for (var value : List.of(justBeyondMaximum, justBeyondMaximum.negate())) {
            var e = assertThrows(IllegalArgumentException.class, () -> encode(Map.of("amount", value)));

            assertThat(e.getMessage(), containsString("Record 1, field 'amount'"));
            assertThat(e.getMessage(), containsString("value exceeds Decimal128's maximum finite integer magnitude"));
            assertThat(e.getMessage(), containsString("store it as a string to keep every digit"));
        }
    }

    @Test
    void shouldRejectBigIntegerOutsideDecimal128RangeWithRecordAndField() {
        var e = assertThrows(IllegalArgumentException.class, () -> encode(Map.of("amount", INTEGER_OUTSIDE_DECIMAL128_RANGE)));

        assertThat(e.getMessage(), containsString("Record 1, field 'amount'"));
        assertThat(e.getMessage(), containsString("value exceeds Decimal128's maximum finite integer magnitude"));
        assertThat(e.getMessage(), containsString("store it as a string to keep every digit"));
    }

    @Test
    void shouldEncodeOffsetDateTimeInsideNestedMap() throws Exception {
        var encoded = encode(Map.of("nested", Map.of("createdAt", OffsetDateTime.parse("2024-01-01T10:00:00+02:00"))));

        var nested = encoded.getDocument("nested");
        assertThat(nested.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(nested.getDateTime("createdAt").getValue(), is(1704096000000L));
    }

    @Test
    void shouldEncodeOffsetDateTimeInsideNestedList() throws Exception {
        var encoded = encode(Map.of("nested", List.of(OffsetDateTime.parse("2024-01-01T10:00:00+02:00"))));

        var nested = encoded.getArray("nested");
        assertThat(nested.get(0).getBsonType(), is(BsonType.DATE_TIME));
        assertThat(nested.get(0).asDateTime().getValue(), is(1704096000000L));
    }

    @Test
    void shouldEncodeOffsetDateTimeInsideListOfNestedMaps() throws Exception {
        var encoded = encode(
            Map.of("nested", List.of(Map.of("createdAt", OffsetDateTime.parse("2024-01-01T10:00:00+02:00"))))
        );

        var nested = encoded.getArray("nested").get(0).asDocument();
        assertThat(nested.get("createdAt").getBsonType(), is(BsonType.DATE_TIME));
        assertThat(nested.getDateTime("createdAt").getValue(), is(1704096000000L));
    }

    @Test
    void shouldEncodeBigIntegerInsideNestedMap() throws Exception {
        var encoded = encode(Map.of("nested", Map.of("amount", NESTED_BIG_INTEGER)));

        assertDecimal128(encoded.getDocument("nested").get("amount"), NESTED_BIG_INTEGER);
    }

    @Test
    void shouldEncodeBigIntegerInsideNestedList() throws Exception {
        var encoded = encode(Map.of("nested", List.of(NESTED_BIG_INTEGER)));

        assertDecimal128(encoded.getArray("nested").get(0), NESTED_BIG_INTEGER);
    }

    @Test
    void shouldEncodeBigIntegerInsideListOfNestedMaps() throws Exception {
        var encoded = encode(Map.of("nested", List.of(Map.of("amount", NESTED_BIG_INTEGER))));

        assertDecimal128(encoded.getArray("nested").get(0).asDocument().get("amount"), NESTED_BIG_INTEGER);
    }

    @Test
    void shouldRejectInvalidBigIntegerInSecondRecordWithNestedFieldPath() {
        var e = assertThrows(
            IllegalArgumentException.class,
            () -> encode(
                Load.builder().build(),
                List.of(
                    Map.of("name", "valid"),
                    Map.of("nested", Map.of("items", List.of(INTEGER_REQUIRING_ROUNDING)))
                )
            )
        );

        assertThat(e.getMessage(), containsString("Record 2, field 'nested.items[0]'"));
        assertThat(e.getMessage(), containsString("MongoDB's Decimal128 driver rejected the exact representation"));
        assertThat(e.getMessage(), containsString("store it as a string to keep every digit"));
        assertThat(e.getCause(), instanceOf(NumberFormatException.class));
    }

    private static void assertDecimal128(BsonValue value, BigInteger expected) {
        assertThat(value.getBsonType(), is(BsonType.DECIMAL128));
        assertThat(value.asDecimal128().getValue().bigDecimalValue().toBigIntegerExact(), is(expected));
    }

    private static void assertDecimal128RoundTrips(BigInteger value) {
        assertThat(decimal128(value).bigDecimalValue().toBigIntegerExact(), is(value));
    }

    private static Decimal128 decimal128(BigInteger value) {
        return new Decimal128(new BigDecimal(value));
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
            .map(model ->
            {
                var document = (Document) ((InsertOneModel<?>) model).getDocument();
                var bsonDocument = document.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
                bsonDocument.size(); // BsonDocumentWrapper encodes lazily; force the driver codecs while inside this helper.
                return bsonDocument;
            })
            .collectList()
            .block();
    }
}
