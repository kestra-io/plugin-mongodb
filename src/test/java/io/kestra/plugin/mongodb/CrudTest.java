package io.kestra.plugin.mongodb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.Document;
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
        var database = databaseName();
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

    @Test
    void deleteManyWithANullFilterLeavesOtherDocuments() throws Exception {
        var database = databaseName();
        try (var client = getMongoClient()) {
            var collection = client.getDatabase(database).getCollection("docs");
            collection.insertOne(new Document("_id", 1).append("k", null));
            collection.insertOne(new Document("_id", 2));
            collection.insertOne(new Document("_id", 3).append("k", "x"));
        }

        var filter = new LinkedHashMap<String, Object>();
        filter.put("k", null);
        var output = Delete.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("docs"))
            .operation(Property.ofValue(Delete.Operation.DELETE_MANY))
            .filter(filter)
            .build()
            .run(this.runContextFactory.of(Map.of()));

        assertThat(output.getDeletedCount(), is(2L));
        try (var client = getMongoClient()) {
            var left = client.getDatabase(database).getCollection("docs").find().first();
            assertThat(left.getInteger("_id"), is(3));
        }
    }

    @Test
    void updateOneSetsAFieldToNull() throws Exception {
        var database = databaseName();
        try (var client = getMongoClient()) {
            client.getDatabase(database).getCollection("docs").insertOne(new Document("_id", 1).append("k", "x"));
        }

        var set = new LinkedHashMap<String, Object>();
        set.put("k", null);
        var document = new LinkedHashMap<String, Object>();
        document.put("$set", set);
        var output = Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("docs"))
            .operation(Property.ofValue(Update.Operation.UPDATE_ONE))
            .filter(Map.of("_id", 1))
            .document(document)
            .build()
            .run(this.runContextFactory.of(Map.of()));

        assertThat(output.getModifiedCount(), is(1L));
        try (var client = getMongoClient()) {
            var stored = client.getDatabase(database).getCollection("docs").find(new Document("_id", 1)).first();
            assertThat(stored.containsKey("k"), is(true));
            assertThat(stored.get("k"), is((Object) null));
        }
    }

    @Test
    void insertOneStoresANullField() throws Exception {
        var database = databaseName();
        var document = new LinkedHashMap<String, Object>();
        document.put("_id", 10);
        document.put("k", null);
        var output = InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("docs"))
            .document(document)
            .build()
            .run(this.runContextFactory.of(Map.of()));

        assertThat(output.getWasAcknowledged(), is(true));
        try (var client = getMongoClient()) {
            var stored = client.getDatabase(database).getCollection("docs").find(new Document("_id", 10)).first();
            assertThat(stored.containsKey("k"), is(true));
            assertThat(stored.get("k"), is((Object) null));
        }
    }

    private static String databaseName() {
        return "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);
    }

    private String insertOneWithId(Object id) throws Exception {
        var database = databaseName();

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
