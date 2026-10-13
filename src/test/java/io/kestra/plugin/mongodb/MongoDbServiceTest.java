package io.kestra.plugin.mongodb;

import java.util.UUID;

import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.UuidRepresentation;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Pins the String form of every kind of document id reported by InsertOne and Update.
 */
class MongoDbServiceTest {
    private static final UUID UUID_VALUE = UUID.fromString("3b241101-e2bb-4255-8caf-4136c566a962");

    @Test
    void scalarIds() {
        assertThat(MongoDbService.toIdString(new BsonObjectId(new ObjectId("60930c39a982931c20ef6cd6"))), is("60930c39a982931c20ef6cd6"));
        assertThat(MongoDbService.toIdString(new BsonString("user-42")), is("user-42"));
        assertThat(MongoDbService.toIdString(new BsonInt32(42)), is("42"));
        assertThat(MongoDbService.toIdString(new BsonInt64(9_007_199_254_740_993L)), is("9007199254740993"));
        assertThat(MongoDbService.toIdString(new BsonDouble(1.5)), is("1.5"));
        assertThat(MongoDbService.toIdString(new BsonDecimal128(Decimal128.parse("1234567890123456789012345678901234"))), is("1234567890123456789012345678901234"));
    }

    @Test
    void standardUuidIsItsCanonicalString() {
        assertThat(MongoDbService.toIdString(new BsonBinary(UUID_VALUE, UuidRepresentation.STANDARD)), is("3b241101-e2bb-4255-8caf-4136c566a962"));
    }

    @Test
    void legacyUuidAndOtherBinariesStayRaw() {
        assertThat(
            MongoDbService.toIdString(new BsonBinary(UUID_VALUE, UuidRepresentation.JAVA_LEGACY)),
            is("{\"$binary\": {\"base64\": \"VUK74gERJDtiqWbFNkGvjA==\", \"subType\": \"03\"}}")
        );
        assertThat(
            MongoDbService.toIdString(new BsonBinary(new byte[] { 1, 2, 3 })),
            is("{\"$binary\": {\"base64\": \"AQID\", \"subType\": \"00\"}}")
        );
    }

    @Test
    void otherTypesAreRelaxedExtendedJson() {
        assertThat(
            MongoDbService.toIdString(new BsonDocument("a", new BsonInt32(1)).append("b", new BsonString("x\"y"))),
            is("{\"a\": 1, \"b\": \"x\\\"y\"}")
        );
        assertThat(MongoDbService.toIdString(new BsonDateTime(1_704_067_200_000L)), is("{\"$date\": \"2024-01-01T00:00:00Z\"}"));
    }
}
