package io.kestra.plugin.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class CrudTest extends MongoDbContainer {

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of("variable", "John Doe"));
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        InsertOne insert = InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .document(
                ImmutableMap.of(
                    "name", "{{ variable }}",
                    "tags", List.of("blue", "green", "red")
                )
            )
            .build();

        InsertOne.Output insertOutput = insert.run(runContext);
        assertThat(insertOutput.getInsertedId() != null, is(true));

        Update update = Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .operation(Property.ofValue(Update.Operation.REPLACE_ONE))
            .document(
                ImmutableMap.of(
                    "name", "{{ variable }}",
                    "tags", List.of("green", "red")
                )
            )
            .filter(
                ImmutableMap.of(
                    "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
                )
            )
            .build();

        Update.Output updateOutput = update.run(runContext);
        assertThat(updateOutput.getModifiedCount(), is(1L));

        update = Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .document("{\"$set\": { \"tags\": [\"blue\", \"green\", \"red\"]}}")
            .filter(
                ImmutableMap.of(
                    "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
                )
            )
            .build();

        updateOutput = update.run(runContext);
        assertThat(updateOutput.getModifiedCount(), is(1L));

        Find find = Find.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .filter(
                ImmutableMap.of(
                    "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
                )
            )
            .build();

        Find.Output findOutput = find.run(runContext);
        assertThat(findOutput.getSize(), is(1L));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("_id"), is(insertOutput.getInsertedId()));

        Delete delete = Delete.builder()
            .connection(
                MongoDbConnection.builder()
                    .uri(Property.ofValue(connectionUri))
                    .build()
            )
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .filter(
                ImmutableMap.of(
                    "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
                )
            )
            .build();

        Delete.Output deleteOutput = delete.run(runContext);
        assertThat(deleteOutput.getDeletedCount(), is(1L));
    }

    @Test
    void insertOneObjectId() throws Exception {
        assertThat(insertOneWithId(ImmutableMap.of("$oid", "60930c39a982931c20ef6cd6")), is("60930c39a982931c20ef6cd6"));
    }

    @Test
    void insertOneStringId() throws Exception {
        assertThat(insertOneWithId("user-42"), is("user-42"));
    }

    @Test
    void insertOneIntegerId() throws Exception {
        assertThat(insertOneWithId(42), is("42"));
    }

    @Test
    void insertOneUuidId() throws Exception {
        assertThat(insertOneWithId(ImmutableMap.of("$uuid", "3b241101-e2bb-4255-8caf-4136c566a962")), is("3b241101-e2bb-4255-8caf-4136c566a962"));
    }

    @Test
    void insertOneLongId() throws Exception {
        assertThat(insertOneWithId(ImmutableMap.of("$numberLong", "9007199254740993")), is("9007199254740993"));
    }

    @Test
    void insertOneLegacyUuidIdStaysRaw() throws Exception {
        var legacyUuid = ImmutableMap.of("$binary", ImmutableMap.of("base64", "VUK74gERJDtiqWbFNkGvjA==", "subType", "03"));

        assertThat(insertOneWithId(legacyUuid), is("{\"$binary\": {\"base64\": \"VUK74gERJDtiqWbFNkGvjA==\", \"subType\": \"03\"}}"));
    }

    @Test
    void insertOneDocumentId() throws Exception {
        assertThat(insertOneWithId(ImmutableMap.of("a", 1, "b", "x")), is("{\"a\": 1, \"b\": \"x\"}"));
    }

    @Test
    void insertOneWithNestedIdWrittenLast() throws Exception {
        var database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
        // no top-level _id and a nested _id as the last field name: the driver failed with an NPE, or for null and MinKey wrote and returned an unreadable id
        var documents = List.of(
            "{\"k\": 1, \"a\": {\"_id\": 1}}",
            "{\"a\": {\"b\": 2, \"_id\": 1}}",
            "{\"a\": [{\"_id\": 1}, 5]}",
            "{\"tag\": \"x\", \"p\": {\"_id\": null}}",
            "{\"tag\": \"x\", \"p\": {\"_id\": {\"$minKey\": 1}}}"
        );

        for (int i = 0; i < documents.size(); i++) {
            var insert = InsertOne.builder()
                .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
                .database(Property.ofValue(database))
                .collection(Property.ofValue("nested_" + i))
                .document(documents.get(i))
                .build();

            var insertOutput = insert.run(runContextFactory.of(Map.of()));

            try (var client = getMongoClient()) {
                var stored = client.getDatabase(database).getCollection("nested_" + i, BsonDocument.class).find().into(new ArrayList<>());
                assertThat(stored, hasSize(1));
                var document = stored.getFirst();
                var id = document.remove("_id");
                assertThat(insertOutput.getInsertedId(), is(id.asObjectId().getValue().toHexString()));
                assertThat(document, is(BsonDocument.parse(documents.get(i))));
            }
        }
    }

    private String insertOneWithId(Object id) throws Exception {
        var database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        var insert = InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .document(ImmutableMap.of("_id", id, "name", "John Doe"))
            .build();

        var insertOutput = insert.run(runContextFactory.of(Map.of()));
        assertThat(insertOutput.getWasAcknowledged(), is(true));

        return insertOutput.getInsertedId();
    }
}
