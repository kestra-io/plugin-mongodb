package io.kestra.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class CrudTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of(ImmutableMap.of("variable", "John Doe"));
        String database = "ut_" + IdUtils.create().toLowerCase(Locale.ROOT);

        InsertOne insert = InsertOne.builder()
            .connection(MongoDbConnection.builder().uri(Property.of("mongodb://root:example@localhost:27017/?authSource=admin")).build())
            .database(Property.of(database))
            .collection(Property.of("insert"))
            .document(ImmutableMap.of(
                "name", "{{ variable }}",
                "tags", List.of("blue", "green", "red")
            ))
            .build();

        InsertOne.Output insertOutput = insert.run(runContext);
        assertThat(insertOutput.getInsertedId() != null, is(true));

        Update update = Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.of("mongodb://root:example@localhost:27017/?authSource=admin")).build())
            .database(Property.of(database))
            .collection(Property.of("insert"))
            .operation(Property.of(Update.Operation.REPLACE_ONE))
            .document(ImmutableMap.of(
                "name", "{{ variable }}",
                "tags", List.of("green", "red")
            ))
            .filter(ImmutableMap.of(
                "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
            ))
            .build();

        Update.Output updateOutput = update.run(runContext);
        assertThat(updateOutput.getModifiedCount(), is(1L));


        update = Update.builder()
            .connection(MongoDbConnection.builder().uri(Property.of("mongodb://root:example@localhost:27017/?authSource=admin")).build())
            .database(Property.of(database))
            .collection(Property.of("insert"))
            .document("{\"$set\": { \"tags\": [\"blue\", \"green\", \"red\"]}}")
            .filter(ImmutableMap.of(
                "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
            ))
            .build();

        updateOutput = update.run(runContext);
        assertThat(updateOutput.getModifiedCount(), is(1L));

        Find find = Find.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.of("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.of(database))
            .collection(Property.of("insert"))
            .filter(ImmutableMap.of(
                "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
            ))
            .build();

        Find.Output findOutput = find.run(runContext);
        assertThat(findOutput.getSize(), is(1L));
        assertThat(((Map<String, Object>) findOutput.getRows().get(0)).get("_id"), is(insertOutput.getInsertedId()));

        Delete delete = Delete.builder()
            .connection(MongoDbConnection.builder()
                .uri(Property.of("mongodb://root:example@localhost:27017/?authSource=admin"))
                .build())
            .database(Property.of(database))
            .collection(Property.of("insert"))
            .filter(ImmutableMap.of(
                "_id", ImmutableMap.of("$oid", insertOutput.getInsertedId())
            ))
            .build();

        Delete.Output deleteOutput = delete.run(runContext);
        assertThat(deleteOutput.getDeletedCount(), is(1L));
    }
}
