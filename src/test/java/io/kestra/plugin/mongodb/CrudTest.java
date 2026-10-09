package io.kestra.plugin.mongodb;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
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

    private String insertOneWithId(Object id) throws Exception {
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        InsertOne insert = InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.ofValue(connectionUri)).build())
            .database(Property.ofValue(database))
            .collection(Property.ofValue("insert"))
            .document(ImmutableMap.of("_id", id, "name", "John Doe"))
            .build();

        InsertOne.Output insertOutput = insert.run(runContextFactory.of(Map.of()));
        assertThat(insertOutput.getWasAcknowledged(), is(true));

        return insertOutput.getInsertedId();
    }
}
